// Importa React y hooks necesarios.
import React, { useState, useEffect } from 'react';
// Importa componentes necesarios.
import { DataTable } from '../components/DataTable';
import { BLDetailsModal } from '../components/BLDetailsModal';
import { ImportCSVModal } from '../components/ImportCSVModal';
// Importa funciones de servicio.
import { fetchBLs, importBLsFromCSV } from '../services/blService';
// Importa componentes adicionales.
import { Filters } from '../components/Filters';
// Importa tipos necesarios.
import type { BL, Container, ContainerViaje, Parada, Tracking, Request } from '../types';
// Importa iconos de la librería lucide-react.
import { Calendar, Package, ExternalLink, Upload } from 'lucide-react';
// Importa el tipo Column para la tabla de datos.
import type { Column } from '../components/DataTable';

// Define las columnas para la tabla de datos.
const columns: Column<BL>[] = [
  {
    header: 'BL Code',
    accessor: 'bl_code',
    render: (value: string) => (
      <div className="flex items-center space-x-2">
        <Package className="w-4 h-4 text-blue-600" />
        <span className="font-medium">{value}</span>
      </div>
    ),
  },
  {
    header: 'Fecha BL',
    accessor: 'fecha_bl',
    render: (value: Date) => (
      <div className="flex items-center space-x-2">
        <Calendar className="w-4 h-4 text-gray-600" />
        <span>{value ? new Date(value).toLocaleDateString() : 'N/A'}</span>
      </div>
    ),
  },
  {
    header: 'Etapa',
    accessor: 'etapa',
  },
  {
    header: 'Nave',
    accessor: 'nave',
  },
  {
    header: 'Estado',
    accessor: 'revisado_con_exito',
    render: (value: boolean) => (
      <span className={`px-2 py-1 rounded-full text-xs ${value ? 'bg-green-100 text-green-800' : 'bg-yellow-100 text-yellow-800'}`}>
        {value ? 'Revisado' : 'Pendiente'}
      </span>
    ),
  },
  {
    header: 'Acciones',
    accessor: 'id',
    render: () => (
      <button className="text-blue-600 hover:text-blue-800 inline-flex items-center space-x-1">
        <ExternalLink className="w-4 h-4" />
        <span>Ver detalles</span>
      </button>
    ),
  },
];

// Define el componente principal de la página de BLs.
export function BLsPage() {
  // Estado para almacenar los BLs.
  const [bls, setBLs] = useState<BL[]>([]);
  // Estado para controlar si los datos están cargando.
  const [isLoading, setIsLoading] = useState(true);
  // Estado para almacenar el BL seleccionado.
  const [selectedBL, setSelectedBL] = useState<BL | null>(null);
  // Estado para almacenar los filtros.
  const [filters, setFilters] = useState<Record<string, any>>({
    bl_code: '',
    naviera_id: '',
    year: '',
    month: '',
  });

  // useEffect para cargar los BLs al montar el componente o cambiar los filtros.
  useEffect(() => {
    const loadBLs = async () => {
      try {
        setIsLoading(true);

        const { year, month } = filters;
        const formattedFilters = {
          ...filters,
          startDate: year && month ? `${year}-${month}-01` : '',
          endDate: year && month
            ? new Date(parseInt(year), parseInt(month), 0).toISOString().split('T')[0]
            : '',
        };

        const data = await fetchBLs(formattedFilters);
        setBLs(data);
      } catch (error) {
        console.error('Failed to fetch BLs:', error);
      } finally {
        setIsLoading(false);
      }
    };

    loadBLs();
  }, [filters]);

  // Maneja el cambio en los filtros.
  const handleFilterChange = (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>) => {
    const { name, value } = e.target;
    setFilters((prevFilters) => ({
      ...prevFilters,
      [name]: value || '',
    }));
  };

  // Mock data para el ejemplo.
  const mockContainers: Container[] = [
    {
      id: '1',
      code: 'CONT123',
      size: 40,
      type: 'DRY',
      estado: 'En tránsito',
    },
  ];

  const mockContainersViaje: ContainerViaje[] = [
    {
      id: '1',
      container_id: '1',
      bl_id: '1',
      peso_kg: 1000,
      localidad_actual: 'Puerto de Shanghai',
    },
  ];

  const mockParadas: Parada[] = [
    {
      id: '1',
      locode: 'CNSHA',
      pais: 'China',
      lugar: 'Shanghai',
    },
  ];

  const mockTracking: Tracking[] = [
    {
      id: '1',
      bl_id: '1',
      fecha: new Date(),
      status: 'En tránsito',
      orden: 1,
      parada_id: '1',
      terminal: 'Terminal 1',
      is_pol: true,
      is_pod: false,
    },
  ];

  const mockRequests: Request[] = [
    {
      id: '1',
      bl_id: '1',
      url: 'https://api.example.com',
      timestamp: new Date(),
      success: true,
      response_code: 200,
      error: '',
      id_html: 1,
    },
  ];

  // Estado para controlar la visibilidad del modal de importación.
  const [isImportModalOpen, setIsImportModalOpen] = useState(false);

  // Maneja la importación de BLs desde un archivo CSV.
  const handleImport = async (data: Partial<BL>[]) => {
    try {
      await importBLsFromCSV(data);
      // Recargar los BLs después de la importación.
      const updatedBLs = await fetchBLs(filters);
      setBLs(updatedBLs);
    } catch (error) {
      console.error('Error importing BLs:', error);
      // Aquí podrías mostrar una notificación de error.
    }
  };

  return (
    <div className="container mx-auto px-4 py-8">
      <div className="mb-8 flex justify-between items-center">
        <div>
          <h1 className="text-3xl font-bold text-gray-900">Bills of Lading</h1>
          <p className="mt-2 text-gray-600">Gestión y seguimiento de BLs</p>
        </div>
        <button
          onClick={() => setIsImportModalOpen(true)}
          className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-blue-600 hover:bg-blue-700"
        >
          <Upload className="w-4 h-4 mr-2" />
          Importar CSV
        </button>
      </div>

      <Filters filters={filters} onFilterChange={handleFilterChange} />

      <div className="mt-6">
        <DataTable 
          data={bls} 
          columns={columns} 
          isLoading={isLoading}
          onRowClick={(bl) => setSelectedBL(bl)}
        />
      </div>

      {selectedBL && (
        <BLDetailsModal
          isOpen={true}
          onClose={() => setSelectedBL(null)}
          bl={selectedBL}
          containers={mockContainers}
          containersViaje={mockContainersViaje}
          paradas={mockParadas}
          tracking={mockTracking}
          requests={mockRequests}
        />
      )}

      <ImportCSVModal
        isOpen={isImportModalOpen}
        onClose={() => setIsImportModalOpen(false)}
        onImport={handleImport}
      />
    </div>
  );
}