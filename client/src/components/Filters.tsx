import React from 'react';

interface FiltersProps {
  filters: Record<string, any>;
  onFilterChange: (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>) => void;
}

export const Filters: React.FC<FiltersProps> = ({ filters, onFilterChange }) => {
  return (
    <div className="p-4 bg-white shadow rounded-lg">
      <h2 className="text-lg font-semibold text-gray-700 mb-4">Filtros</h2>
      <div className="flex flex-wrap items-center gap-4">
        <input
          type="text"
          placeholder="Buscar por código de BL"
          className="flex-1 border border-gray-300 rounded-lg px-4 py-2 text-sm focus:outline-none focus:ring focus:ring-blue-300"
          name="bl_code"
          value={filters.bl_code || ''}
          onChange={onFilterChange}
        />
        <select
          name="naviera_id"
          className="flex-1 border border-gray-300 rounded-lg px-4 py-2 text-sm focus:outline-none focus:ring focus:ring-blue-300"
          value={filters.naviera_id || ''}
          onChange={onFilterChange}
        >
          <option value="">Todas las navieras</option>
          <option value="1">Maersk</option>
          <option value="2">MSC</option>
          <option value="3">CMA CGM</option>
          {/* Añadir más opciones */}
        </select>
      </div>
    </div>
  );
};
