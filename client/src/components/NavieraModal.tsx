import { useState } from 'react';
import { X } from 'lucide-react';
import { DataTable } from './DataTable';
import { BLDetailsModal } from './BLDetailsModal';
import type { BL, Container, ContainerViaje, Parada, Tracking, Request } from '../types';

// Definición de las propiedades que acepta el componente NavieraModal
interface NavieraModalProps {
  isOpen: boolean; // Determina si el modal está abierto o no
  onClose: () => void; // Función para cerrar el modal
  naviera: { // Información de la naviera seleccionada
    id: string;
    name: string;
  };
  bls: BL[]; // Lista de BLs asociados a la naviera
}

// Componente principal del modal para mostrar información de la naviera
export function NavieraModal({ isOpen, onClose, naviera, bls }: NavieraModalProps) {
  const [selectedBL, setSelectedBL] = useState<BL | null>(null); // Estado para almacenar el BL seleccionado

  // Si el modal no está abierto, no renderiza nada
  if (!isOpen) return null;

  // Definición de las columnas de la tabla
  const columns = [
    {
      header: 'BL Code', // Encabezado de la columna
      accessor: 'bl_code' as keyof BL, // Propiedad del objeto BL para acceder a los datos
    },
    {
      header: 'Fecha BL',
      accessor: 'fecha_bl' as keyof BL,
      render: (value: Date) => new Date(value).toLocaleDateString(), // Formatea la fecha para mostrarla
    },
    {
      header: 'Etapa',
      accessor: 'etapa' as keyof BL,
    },
    {
      header: 'Nave',
      accessor: 'nave' as keyof BL,
    },
    {
      header: 'Estado',
      accessor: 'revisado_con_exito' as keyof BL,
      render: (value: boolean) => (
        <span className={`px-2 py-1 rounded-full text-xs ${value ? 'bg-green-100 text-green-800' : 'bg-yellow-100 text-yellow-800'}`}>
          {value ? 'Revisado' : 'Pendiente'} {/* Muestra si el BL ha sido revisado */}
        </span>
      ),
    },
    {
      header: 'Acciones',
      accessor: 'id' as keyof BL,
      render: (_: any, bl?: BL) => bl && (
        <button
          onClick={() => setSelectedBL(bl)} // Selecciona un BL para mostrar detalles
          className="text-blue-600 hover:text-blue-800 text-sm font-medium"
        >
          Ver detalles {/* Botón para abrir el modal de detalles del BL */}
        </button>
      ),
    },
  ];

  // Datos simulados para el ejemplo (mock data)
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

  return (
    <>
      <div className="fixed inset-0 bg-black bg-opacity-50 z-50 flex items-center justify-center">
        {/* Contenedor principal del modal */}
        <div className="bg-white rounded-lg shadow-xl w-full max-w-4xl max-h-[90vh] overflow-hidden">
          {/* Encabezado del modal */}
          <div className="p-4 border-b border-gray-200 flex items-center justify-between">
            <h2 className="text-xl font-semibold text-gray-900">
              BLs de {naviera.name} {/* Título del modal */}
            </h2>
            <button
              onClick={onClose} // Cierra el modal
              className="p-1 hover:bg-gray-100 rounded-full"
            >
              <X className="w-5 h-5 text-gray-500" /> {/* Ícono para cerrar */}
            </button>
          </div>
          {/* Cuerpo del modal */}
          <div className="p-4 overflow-auto max-h-[calc(90vh-8rem)]">
            <DataTable
              data={bls} // Datos de los BLs para la tabla
              columns={columns} // Definición de las columnas
              isLoading={false} // Indica si la tabla está cargando
            />
          </div>
        </div>
      </div>

      {/* Modal de detalles del BL seleccionado */}
      {selectedBL && (
        <BLDetailsModal
          isOpen={true} // Siempre abierto si hay un BL seleccionado
          onClose={() => setSelectedBL(null)} // Cierra el modal de detalles
          bl={selectedBL} // BL seleccionado
          containers={mockContainers} // Datos simulados de contenedores
          containersViaje={mockContainersViaje} // Datos simulados de contenedores en viaje
          paradas={mockParadas} // Datos simulados de paradas
          tracking={mockTracking} // Datos simulados de seguimiento
          requests={mockRequests} // Datos simulados de requests
        />
      )}
    </>
  );
}
