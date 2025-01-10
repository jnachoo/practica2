import React from 'react';

// Define el tipo de las propiedades que recibe el componente.
interface DataTableProps<T> {
  data: T[];
  columns: Column<T>[];
  isLoading?: boolean;
  onRowClick?: (item: T) => void;
}

// Define el tipo de las columnas.
export interface Column<T> {
  header: string;
  accessor: keyof T;
  render?: (value: any, item: T) => React.ReactNode;
}

// Define el componente DataTable.
export function DataTable<T>({ data, columns, onRowClick }: DataTableProps<T>) {
  return (
    <div className="shadow overflow-hidden border-b border-gray-200 sm:rounded-lg">
      <table className="min-w-full divide-y divide-gray-200">
        <thead className="bg-gray-50">
          <tr>
            {columns.map((column, index) => (
              <th
                key={index}
                scope="col"
                className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider"
              >
                {column.header}
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="bg-white divide-y divide-gray-200">
          {data.map((item, rowIndex) => (
            <tr
              key={rowIndex}
              onClick={() => onRowClick?.(item)}
              className={onRowClick ? 'cursor-pointer hover:bg-gray-50' : ''}
            >
              {columns.map((column, colIndex) => (
                <td key={colIndex} className="px-6 py-4 whitespace-nowrap">
                  {column.render
                    ? column.render(item[column.accessor], item)
                    : String(item[column.accessor] ?? '')}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}