import { useState } from 'react';
import { X, Package, MapPin, ExternalLink } from 'lucide-react';
import { DataTable } from './DataTable';
import type { BL, Container, ContainerViaje, Parada, Tracking, Request } from '../types';

// Define las propiedades que recibe el componente.
interface BLDetailsModalProps {
  isOpen: boolean;
  onClose: () => void;
  bl: BL;
  containers: Container[];
  containersViaje: ContainerViaje[];
  paradas: Parada[];
  tracking: Tracking[];
  requests: Request[];
}

// Define el componente BLDetailsModal.
export function BLDetailsModal({
  isOpen,
  onClose,
  bl,
  containers,
  containersViaje,
  paradas,
  tracking,
  requests
}: BLDetailsModalProps) {
  // Estado para controlar la pestaña activa.
  const [activeTab, setActiveTab] = useState<'containers' | 'tracking' | 'requests'>('containers');

  // Si el modal no está abierto, no renderiza nada.
  if (!isOpen) return null;

  // Define las columnas para la tabla de containers.
  const containerColumns = [
    {
      header: 'Código',
      accessor: 'code' as keyof Container,
    },
    {
      header: 'Tamaño',
      accessor: 'size' as keyof Container,
    },
    {
      header: 'Tipo',
      accessor: 'type' as keyof Container,
    },
    {
      header: 'Estado',
      accessor: 'estado' as keyof Container,
    },
    {
      header: 'Localidad Actual',
      accessor: 'id' as keyof Container,
      render: (containerId: string) => {
        const containerViaje = containersViaje.find(cv => cv.container_id === containerId);
        return containerViaje?.localidad_actual || '-';
      },
    },
  ];

  // Define las columnas para la tabla de tracking.
  const trackingColumns = [
    {
      header: 'Fecha',
      accessor: 'fecha' as keyof Tracking,
      render: (value: Date) => new Date(value).toLocaleDateString(),
    },
    {
      header: 'Status',
      accessor: 'status' as keyof Tracking,
    },
    {
      header: 'Terminal',
      accessor: 'terminal' as keyof Tracking,
    },
    {
      header: 'Parada',
      accessor: 'parada_id' as keyof Tracking,
      render: (paradaId: string) => {
        const parada = paradas.find(p => p.id === paradaId);
        return parada ? `${parada.lugar}, ${parada.pais}` : '-';
      },
    },
  ];

  // Define las columnas para la tabla de requests.
  const requestColumns = [
    {
      header: 'Código',
      accessor: 'response_code' as keyof Request,
    },
    {
      header: 'Error',
      accessor: 'error' as keyof Request,
    },
  ];

  return (
    // Contenedor principal del modal, ocupa toda la pantalla con un fondo semitransparente.
    <div className="fixed inset-0 bg-black bg-opacity-50 z-50 flex items-center justify-center">
      {/* Contenedor del contenido del modal, con fondo blanco, bordes redondeados y sombra. */}
      <div className="bg-white rounded-lg shadow-xl w-full max-w-6xl max-h-[90vh] overflow-hidden">
        {/* Encabezado del modal con título y botón de cierre. */}
        <div className="p-4 border-b border-gray-200 flex items-center justify-between">
          <div>
            <h2 className="text-xl font-semibold text-gray-900">
              Detalles del BL: {bl.bl_code}
            </h2>
            <p className="text-sm text-gray-500 mt-1">
              Nave: {bl.nave} | Fecha: {bl.fecha_bl ? new Date(bl.fecha_bl).toLocaleDateString() : '-'}
            </p>
          </div>
          <button
            onClick={onClose}
            className="p-1 hover:bg-gray-100 rounded-full"
          >
            <X className="w-5 h-5 text-gray-500" />
          </button>
        </div>

        {/* Navegación de pestañas dentro del modal. */}
        <div className="border-b border-gray-200">
          <nav className="flex space-x-4 px-4" aria-label="Tabs">
            <button
              onClick={() => setActiveTab('containers')}
              className={`px-3 py-2 text-sm font-medium rounded-md ${
                activeTab === 'containers'
                  ? 'bg-blue-50 text-blue-700'
                  : 'text-gray-500 hover:text-gray-700'
              }`}
            >
              <Package className="w-4 h-4 inline-block mr-2" />
              Containers
            </button>
            <button
              onClick={() => setActiveTab('tracking')}
              className={`px-3 py-2 text-sm font-medium rounded-md ${
                activeTab === 'tracking'
                  ? 'bg-blue-50 text-blue-700'
                  : 'text-gray-500 hover:text-gray-700'
              }`}
            >
              <MapPin className="w-4 h-4 inline-block mr-2" />
              Tracking
            </button>
            <button
              onClick={() => setActiveTab('requests')}
              className={`px-3 py-2 text-sm font-medium rounded-md ${
                activeTab === 'requests'
                  ? 'bg-blue-50 text-blue-700'
                  : 'text-gray-500 hover:text-gray-700'
              }`}
            >
              <ExternalLink className="w-4 h-4 inline-block mr-2" />
              Requests
            </button>
          </nav>
        </div>

        {/* Contenido de las pestañas, se muestra según la pestaña activa. */}
        <div className="p-4 overflow-y-auto">
          {activeTab === 'containers' && (
            <DataTable data={containers} columns={containerColumns} />
          )}
          {activeTab === 'tracking' && (
            <DataTable data={tracking} columns={trackingColumns} />
          )}
          {activeTab === 'requests' && (
            <DataTable data={requests} columns={requestColumns} />
          )}
        </div>
      </div>
    </div>
  );
}