import React, { useState, useMemo } from 'react';
import Papa from 'papaparse';
import { Card, CardContent } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Button } from '@/components/ui/button';

// Supported stat categories
const CATEGORIES = ['batting', 'baserunning', 'fielding', 'pitching'];

export default function App() {
  // Parsed data: { STATE: { category: rows[] } }
  const [dataStore, setDataStore] = useState({});
  const [states, setStates] = useState([]);
  const [selectedState, setSelectedState] = useState('');
  const [selectedCategory, setSelectedCategory] = useState(CATEGORIES[0]);
  const [filterText, setFilterText] = useState('');
  const [sortConfig, setSortConfig] = useState({ key: '', direction: 'asc' });

  // Handle CSV file uploads
  const handleFiles = e => {
    const files = Array.from(e.target.files);
    files.forEach(file => {
      Papa.parse(file, {
        header: true,
        skipEmptyLines: true,
        complete: result => {
          const name = file.name; // e.g. AK_batting_stats.csv
          const parts = name.split('_');
          if (parts.length < 3) return;
          const state = parts[0].toUpperCase();
          const category = parts[1].toLowerCase();
          if (!CATEGORIES.includes(category)) return;

          setDataStore(prev => {
            const updated = { ...prev };
            if (!updated[state]) updated[state] = {};
            updated[state][category] = result.data;
            return updated;
          });
          setStates(prev => {
            const list = Array.from(new Set([...prev, state]));
            // default to first state if none selected
            if (!selectedState) setSelectedState(state);
            return list;
          });
        }
      });
    });
  };

  // Derived display data: filter and sort
  const displayedData = useMemo(() => {
    if (!selectedState || !dataStore[selectedState]) return [];
    const rows = dataStore[selectedState][selectedCategory] || [];
    let filtered = rows;
    if (filterText) {
      const ft = filterText.toLowerCase();
      filtered = filtered.filter(row =>
        Object.values(row).some(val =>
          String(val).toLowerCase().includes(ft)
        )
      );
    }
    if (sortConfig.key) {
      const { key, direction } = sortConfig;
      filtered = [...filtered].sort((a, b) => {
        const aVal = a[key];
        const bVal = b[key];
        if (!isNaN(aVal) && !isNaN(bVal)) {
          return direction === 'asc' ? aVal - bVal : bVal - aVal;
        }
        return direction === 'asc'
          ? String(aVal).localeCompare(String(bVal))
          : String(bVal).localeCompare(String(aVal));
      });
    }
    return filtered;
  }, [dataStore, selectedState, selectedCategory, filterText, sortConfig]);

  const requestSort = colKey => {
    setSortConfig(prev => {
      const direction = prev.key === colKey && prev.direction === 'asc' ? 'desc' : 'asc';
      return { key: colKey, direction };
    });
  };

  return (
    <div className="min-h-screen bg-gray-50 p-6">
      <Card className="max-w-7xl mx-auto">
        <CardContent>
          <div className="mb-4">
            <Input
              type="file"
              multiple
              accept=".csv"
              onChange={handleFiles}
            />
          </div>

          {states.length > 0 && (
            <div className="flex flex-wrap items-center justify-between mb-4 space-y-2">
              <div className="flex space-x-2">
                {states.map(state => (
                  <Button
                    key={state}
                    variant={selectedState === state ? 'default' : 'outline'}
                    onClick={() => setSelectedState(state)}
                  >
                    {state}
                  </Button>
                ))}
              </div>
              <div className="flex space-x-2">
                {CATEGORIES.map(cat => (
                  <Button
                    key={cat}
                    variant={selectedCategory === cat ? 'default' : 'outline'}
                    onClick={() => setSelectedCategory(cat)}
                  >
                    {cat.charAt(0).toUpperCase() + cat.slice(1)}
                  </Button>
                ))}
              </div>
              <Input
                placeholder="Search..."
                value={filterText}
                onChange={e => setFilterText(e.target.value)}
                className="w-1/3"
              />
            </div>
          )}

          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-100">
                <tr>
                  {displayedData[0] && Object.keys(displayedData[0]).map(col => (
                    <th
                      key={col}
                      onClick={() => requestSort(col)}
                      className="px-4 py-2 text-left cursor-pointer select-none"
                    >
                      {col} {sortConfig.key === col ? (sortConfig.direction === 'asc' ? '🔼' : '🔽') : ''}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-200">
                {displayedData.map((row, i) => (
                  <tr key={i} className="hover:bg-gray-50">
                    {Object.values(row).map((val, j) => (
                      <td key={j} className="px-4 py-2 whitespace-nowrap">
                        {val}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
