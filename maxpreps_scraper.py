import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm
import logging
import time
from typing import Dict, List, Optional
from urllib.parse import urljoin
import re
from bs4 import Tag, NavigableString

class MaxPrepsScraper:
    def __init__(self):
        """Initialize the scraper."""
        self.base_url = 'https://www.maxpreps.com'
        self.output_dir = 'output'
        self.teams_dir = 'teams'
        self.current_team = ''
        self.current_url = ''
        self.state_abbr = ''  # Will be set when processing teams
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self._setup_logging()
        self._setup_directories()
        self.logger = logging.getLogger(__name__)
        
        # Create category subdirectories
        self.batting_dir = os.path.join(self.output_dir, 'batting')
        self.baserunning_dir = os.path.join(self.output_dir, 'baserunning')
        self.fielding_dir = os.path.join(self.output_dir, 'fielding')
        self.pitching_dir = os.path.join(self.output_dir, 'pitching')
        
        # Create subdirectories
        os.makedirs(self.batting_dir, exist_ok=True)
        os.makedirs(self.baserunning_dir, exist_ok=True)
        os.makedirs(self.fielding_dir, exist_ok=True)
        os.makedirs(self.pitching_dir, exist_ok=True)
        os.makedirs(self.teams_dir, exist_ok=True)
        
        self.logger.info(f"Output directories set to: {os.path.abspath(self.output_dir)}")

    def _setup_logging(self):
        """Set up logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('scraper.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def _setup_directories(self):
        """Set up output directories."""
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.teams_dir, exist_ok=True)
        for category in ['batting', 'baserunning', 'fielding', 'pitching']:
            os.makedirs(os.path.join(self.output_dir, category), exist_ok=True)
        self.logger.info(f"Output directory set to: {os.path.abspath(self.output_dir)}")

    def _get_soup(self, url: str) -> Optional[BeautifulSoup]:
        """Get BeautifulSoup object from URL"""
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            return BeautifulSoup(response.text, 'html.parser')
        except requests.RequestException as e:
            self.logger.error(f"Failed to fetch {url}: {str(e)}")
            return None

    def _normalize_headers(self, headers: List[str]) -> List[str]:
        """Normalize table headers for consistency"""
        header_map = {
            'BATTING AVG': 'AVG', 'BA': 'AVG',
            'RUNS BATTED IN': 'RBI', 'RBI': 'RBI',
            'RUNS SCORED': 'R', 'R': 'R',
            'AT BATS': 'AB', 'AB': 'AB',
            'HITS': 'H', 'H': 'H',
            'DOUBLES': '2B', '2B': '2B',
            'TRIPLES': '3B', '3B': '3B',
            'HOME RUNS': 'HR', 'HR': 'HR',
            'WALKS': 'BB', 'BB': 'BB',
            'STRIKEOUTS': 'K', 'K': 'K',
            'OPPONENT AVG': 'OBA', 'OPP BA': 'OBA', 'OPP AVG': 'OBA',
            'ON BASE %': 'OBP', 'ON BASE PCT': 'OBP',
            'WILD PITCHES': 'WP',
            'HIT BATTERS': 'HBP',
            'SACRIFICE FLIES': 'SF',
            'SACRIFICE HITS': 'SH/B',
            'PITCH COUNT': '#P', 'PITCHES': '#P',
            'BALKS': 'BK',
            'PICKOFFS': 'PO',
            'STOLEN BASES AGAINST': 'SB',
            'EARNED RUN AVERAGE': 'ERA',
            'INNINGS PITCHED': 'IP',
            'FIELDING PERCENTAGE': 'FLD%',
            'PUTOUTS': 'PO',
            'ASSISTS': 'A',
            'ERRORS': 'E',
            'DOUBLE PLAYS': 'DP',
            'STOLEN BASES': 'SB',
            'CAUGHT STEALING': 'CS',
            'GAMES PLAYED': 'GP',
            'GAMES STARTED': 'GS',
            'APPEARANCES': 'APP'
        }
        
        normalized = []
        for header in headers:
            header = header.strip().upper()
            normalized.append(header_map.get(header, header))
        return normalized

    def _determine_table_type(self, table_or_df) -> Optional[str]:
        """Determine the type of stats table based on its headers."""
        try:
            # Get headers from either DataFrame or Tag
            if isinstance(table_or_df, pd.DataFrame):
                headers = [col.strip().upper() for col in table_or_df.columns]
            else:
                # Get headers from the table Tag
                header_row = table_or_df.find('tr')
                if not header_row or isinstance(header_row, NavigableString):
                    return None
                headers = [th.get_text(strip=True).upper() for th in header_row.find_all(['th', 'td'])]
            
            if not headers:
                return None

            # Define required indicators for each table type
            # For pitching, we require ERA or IP plus at least one other pitching stat
            pitching_basic = {'ERA', 'W', 'L', 'W%', 'APP'}
            pitching_advanced = {'IP', 'H', 'R', 'ER', 'BB', 'SO'}
            pitching_additional = {'OBA', 'WP', 'HBP', 'BK'}
            
            # For batting, we require AVG or AB plus at least one other batting stat
            batting_required = {'AVG', 'AB', 'H', 'R', 'RBI'}
            batting_additional = {'2B', '3B', 'HR', 'BB', 'K', 'HBP', 'SF', 'SH', 'ROE', 'FC', 'LOB', 'OBP', 'SLG', 'OPS'}
            
            # For fielding, we require FPCT or TC plus at least one other fielding stat
            fielding_required = {'FPCT', 'TC', 'FLD%'}
            fielding_additional = {'PO', 'A', 'E', 'DP'}
            
            # For baserunning, we require SB or CS
            baserunning_required = {'SB', 'CS', 'SBA'}

            # Check for pitching first (most specific)
            headers_set = set(headers)
            if (len(pitching_basic & headers_set) >= 3 or 
                len(pitching_advanced & headers_set) >= 3 or 
                len(pitching_additional & headers_set) >= 2):
                return 'pitching'
            
            # Check for fielding next
            if any(req in headers for req in fielding_required) and any(add in headers for add in fielding_additional):
                return 'fielding'
            
            # Check for baserunning
            if any(req in headers for req in baserunning_required):
                return 'baserunning'
            
            # Check for batting last (most general)
            # If we have any of the required batting stats and any additional batting stats
            if (len(batting_required & headers_set) >= 2 or 
                len(batting_additional & headers_set) >= 2):
                return 'batting'

            return None
        except Exception as e:
            self.logger.error(f"Error determining table type: {e}")
            return None

    def _get_print_url(self, team_url: str) -> Optional[str]:
        """Get the print URL for a team's stats page."""
        try:
            # For Caesar Rodney Riders, we know the exact print URL
            if 'caesar-rodney-riders' in team_url:
                return "https://www.maxpreps.com/print/team_stats.aspx?admin=0&bygame=0&league=0&print=1&schoolid=a4de46de-cc0c-4f6e-aefb-da009a02b735&ssid=b231ef20-6494-421f-b3a0-9ccbb82d678f"
            
            # For other teams, try to find the print link
            response = requests.get(team_url, headers=self.headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Look for print link in various places
            print_link = None
            for link in soup.find_all('a'):
                href = link.get('href', '')
                if isinstance(href, str) and 'print/team_stats.aspx' in href:
                    print_link = href
                    break
                
            if print_link:
                return urljoin('https://www.maxpreps.com', print_link)
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting print URL: {str(e)}")
            return None

    def _normalize_url(self, url: str) -> str:
        """Normalize the team URL."""
        if not url.startswith(('http://', 'https://')):
            url = f'https://www.maxpreps.com{url}'
        return url.replace('http://', 'https://')

    def _get_tables(self, url: str) -> List[Tag]:
        """Get all tables from the given URL."""
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            return soup.find_all('table')
        except Exception as e:
            self.logger.error(f"Error fetching tables: {e}")
            return []

    def _extract_table_data(self, table: Tag) -> pd.DataFrame:
        """Extract data from a table into a DataFrame."""
        try:
            # Get headers
            headers = []
            header_row = table.find('tr')
            if header_row and not isinstance(header_row, NavigableString):
                headers = [th.get_text(strip=True) for th in header_row.find_all(['th', 'td'])]

            # Get data rows
            data = []
            for row in table.find_all('tr')[1:]:  # Skip header row
                if not isinstance(row, NavigableString):
                    row_data = [td.get_text(strip=True) for td in row.find_all(['td', 'th'])]
                    if row_data:  # Only add non-empty rows
                        # Skip rows that contain "Season" or "Totals" in the athlete name (usually second column)
                        if len(row_data) > 1 and not any(x in row_data[1].lower() for x in ['season', 'totals']):
                            data.append(row_data)

            # Create DataFrame
            if headers and data:
                df = pd.DataFrame(data, columns=headers)
                return df
            return pd.DataFrame()
        except Exception as e:
            self.logger.error(f"Error extracting table data: {e}")
            return pd.DataFrame()

    def _identify_pitching_table_type(self, df: pd.DataFrame) -> Optional[str]:
        """Identify the type of pitching table based on its columns."""
        columns = set(col.strip().upper() for col in df.columns)
        
        # Basic pitching stats
        if {'ERA', 'W', 'L', 'W%', 'APP'}.issubset(columns):
            return 'basic'
        # Advanced pitching stats
        elif {'IP', 'H', 'R', 'ER', 'BB', 'SO'}.issubset(columns):
            return 'advanced'
        # Additional pitching stats
        elif {'OBA', 'WP', 'HBP', 'BK'}.issubset(columns):
            return 'additional'
        return None

    def _merge_tables_by_type(self, tables: List[pd.DataFrame], team_name: str, team_url: str, city_state: str) -> Dict[str, pd.DataFrame]:
        """Merge tables of the same type, properly handling duplicate columns."""
        table_groups = {'batting': [], 'pitching': [], 'fielding': [], 'baserunning': []}
        
        for df in tables:
            if df is not None and not df.empty:
                table_type = self._determine_table_type(df)
                if table_type:
                    # Normalize column names
                    df.columns = [col.strip().upper() for col in df.columns]
                    table_groups[table_type].append(df)
        
        merged_tables = {}
        for table_type, dfs in table_groups.items():
            if not dfs:
                continue
                
            # Start with the first table
            merged_df = dfs[0].copy()
            key_columns = ['#', 'ATHLETE NAME']
            
            # For subsequent tables, merge on player info
            for df in dfs[1:]:
                # Remove duplicate key columns from the right table
                right_columns = [col for col in df.columns if col not in key_columns]
                if right_columns:
                    merged_df = pd.merge(
                        merged_df,
                        df[key_columns + right_columns],
                        on=key_columns,
                        how='outer'
                    )
            
            # Add team information
            merged_df['TEAM'] = team_name
            merged_df['CITY_STATE'] = city_state
            merged_df['STATS_URL'] = team_url
            
            merged_tables[table_type] = merged_df
            
        return merged_tables

    def _save_team_without_stats(self, team_name: str, stats_url: str, reason: str, tables_found: int = 0):
        """Save team information to the teams without stats file."""
        with open(self.teams_without_stats_file, 'a', encoding='utf-8') as f:
            f.write(f'{team_name},{stats_url},{reason},{tables_found}\n')

    def process_team(self, team_info: Dict) -> None:
        """Process a single team's statistics."""
        team_name = team_info['school']  # Define team_name at the start
        team_url = team_info['stats_url']  # Define team_url at the start
        try:
            city_state = team_info['city_state']
            
            self.logger.info(f"Processing {team_name}")
            
            # Get print page URL
            print_url = self._get_print_url(team_url)
            if not print_url:
                self._save_team_without_stats(team_name, team_url, "No print URL found", 0)
                self.logger.error(f"Could not get print URL for {team_name}")
                return
            
            # Save HTML in state-specific folder
            state_teams_dir = os.path.join(self.teams_dir, self.state_abbr.upper())
            html_file = os.path.join(state_teams_dir, f"{team_name.replace(' ', '_')}.html")
            response = requests.get(print_url, headers=self.headers)
            
            if response.status_code == 404:
                self._save_team_without_stats(team_name, team_url, "404 Error", 0)
                self.logger.error(f"404 error for {team_name}")
                return
                
            response.raise_for_status()
            
            with open(html_file, 'w', encoding='utf-8') as f:
                f.write(response.text)
            
            self.logger.info(f"Saved HTML for {team_name} to {html_file}")
            
            soup = BeautifulSoup(response.text, 'html.parser')
            tables = soup.find_all('table')
            
            if not tables:
                self._save_team_without_stats(team_name, team_url, "No tables found", 0)
                self.logger.warning(f"No tables found for {team_name}")
                return
            
            if len(tables) < 7:
                self._save_team_without_stats(team_name, team_url, "Insufficient tables", len(tables))
                self.logger.warning(f"Only {len(tables)} tables found for {team_name}")
                return
            
            self.logger.info(f"Found {len(tables)} tables for {team_name}")
            
            # Extract data from all tables
            table_dfs = []
            for table in tables:
                try:
                    df = self._extract_table_data(table)
                    if df is not None and not df.empty:
                        table_dfs.append(df)
                except Exception as e:
                    self.logger.error(f"Error processing table for {team_name}: {str(e)}")
                    continue
            
            # Merge tables by type
            merged_tables = self._merge_tables_by_type(table_dfs, team_name, team_url, city_state)
            
            # Save merged tables
            for table_type, df in merged_tables.items():
                output_file = os.path.join(self.output_dir, table_type, f'{self.state_abbr}_{table_type}_stats.csv')
                df.to_csv(output_file, mode='a', header=not os.path.exists(output_file), index=False)
                self.logger.info(f"Saved {len(df)} rows to {output_file}")
            
            self.logger.info(f"Completed processing {team_name}")
            
        except Exception as e:
            self._save_team_without_stats(team_name, team_url, f"Error: {str(e)}", 0)
            self.logger.error(f"Error processing team {team_name}: {str(e)}")

    def process_state_teams(self, lookup_file: str, state_abbr: str) -> None:
        """Process all teams from a specific state using the lookup file."""
        try:
            self.state_abbr = state_abbr.lower()
            
            # Create state-specific directory for HTML files
            state_teams_dir = os.path.join(self.teams_dir, self.state_abbr.upper())
            os.makedirs(state_teams_dir, exist_ok=True)
            
            # Initialize teams without stats file for this state
            self.teams_without_stats_file = f'{self.state_abbr}_teams_without_stats.csv'
            with open(self.teams_without_stats_file, 'w', encoding='utf-8') as f:
                f.write('team_name,stats_url,reason,tables_found\n')
            
            # Clear existing stat files for this state
            for category in ['batting', 'baserunning', 'fielding', 'pitching']:
                output_file = os.path.join(self.output_dir, category, f'{self.state_abbr}_{category}_stats.csv')
                if os.path.exists(output_file):
                    os.remove(output_file)
            
            # Read the lookup table
            teams_df = pd.read_csv(lookup_file)
            
            # Filter for teams from the specified state
            state_teams = teams_df[teams_df['abbr'] == self.state_abbr]
            
            if len(state_teams) == 0:
                self.logger.error(f"No teams found for state {state_abbr}")
                return
            
            self.logger.info(f"Found {len(state_teams)} teams to process for {state_abbr}")
            
            # Process each team
            for _, team_info in tqdm(state_teams.iterrows(), total=len(state_teams)):
                self.process_team(team_info.to_dict())
                time.sleep(1)  # Add delay to avoid rate limiting
            
            self.logger.info(f"Completed processing all {state_abbr} teams")
            
        except Exception as e:
            self.logger.error(f"Error processing {state_abbr} teams: {str(e)}")

if __name__ == "__main__":
    # You can change the state abbreviation here
    STATE_ABBR = "pa"  # For STATE
    
    # Debug: examine the STATE teams data
    teams_df = pd.read_csv('maxpreps_varisty_baseball_lu_table')
    pa_teams = teams_df[teams_df['abbr'] == 'pa']
    print("\nPennsylvania teams found:")
    print(pa_teams)
    print("\nTotal Pennsylvania teams:", len(pa_teams))
    
    scraper = MaxPrepsScraper()
    scraper.process_state_teams('maxpreps_varisty_baseball_lu_table', STATE_ABBR) 