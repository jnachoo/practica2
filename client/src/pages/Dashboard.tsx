import { useState, useEffect } from 'react'; // Importa hooks de React para manejar el estado y efectos secundarios.
import { 
  Ship, 
  Package, 
  AlertTriangle, 
  Clock, 
  CheckCircle2, 
  XCircle,
  AlertCircle,
} from 'lucide-react'; // Importa iconos de la librería lucide-react.
import { fetchBLs } from '../services/blService'; // Importa la función fetchBLs del servicio blService.
import { NAVIERAS } from '../constants/navieras'; // Importa la constante NAVIERAS del archivo de constantes.
import { NavieraModal } from '../components/NavieraModal'; // Importa el componente NavieraModal.
import { StatCard } from '../components/StatCard'; // Importa el componente StatCard.
import { ProgressBar } from '../components/ProgressBar'; // Importa el componente ProgressBar.
import { StatusBreakdown } from '../components/StatusBreakdown'; // Importa el componente StatusBreakdown.
import type { BL } from '../types'; // Importa el tipo BL.

export function Dashboard() {
  // Estado para almacenar los BLs (Bills of Lading).
  const [bls, setBLs] = useState<BL[]>([]);
  // Estado para almacenar la naviera seleccionada.
  const [selectedNaviera, setSelectedNaviera] = useState<{ id: string; name: string } | null>(null);
  // Estado para controlar la visibilidad del modal.
  const [isModalOpen, setIsModalOpen] = useState(false);
  // Estado para controlar si los datos están cargando.
  const [, setIsLoading] = useState(true);

  // useEffect para cargar los BLs al montar el componente.
  useEffect(() => {
    const loadBLs = async () => {
      try {
        const data = await fetchBLs(); // Llama a la función fetchBLs para obtener los BLs.
        setBLs(data); // Actualiza el estado con los BLs obtenidos.
      } catch (error) {
        console.error('Failed to fetch BLs:', error); // Maneja errores en la llamada a la API.
      } finally {
        setIsLoading(false); // Indica que la carga ha terminado.
      }
    };

    loadBLs(); // Llama a la función para cargar los BLs.
  }, []);

  // Calcula el total de BLs.
  const totalBLs = bls.length;
  // Calcula el número de BLs pendientes de revisión.
  const pendingReview = bls.filter(bl => !bl.revisado_con_exito).length;
  // Calcula el número de BLs completados hoy.
  const completedToday = bls.filter(bl => bl.revisado_hoy).length;
  // Calcula el número de BLs con errores.
  const withErrors = bls.filter(bl => bl.manual_pendiente).length;

  // Datos para el desglose de estado.
  const statusData = [
    {
      label: 'En tránsito',
      count: bls.filter(bl => bl.etapa === 'En tránsito').length,
      color: 'bg-blue-600'
    },
    {
      label: 'Descargado',
      count: bls.filter(bl => bl.etapa === 'Descargado').length,
      color: 'bg-green-600'
    },
    {
      label: 'Pendiente',
      count: bls.filter(bl => bl.etapa === 'Pendiente').length,
      color: 'bg-yellow-600'
    }
  ];

  // Maneja el clic en una naviera.
  const handleNavieraClick = (naviera: { id: string; name: string }) => {
    setSelectedNaviera(naviera); // Actualiza la naviera seleccionada.
    setIsModalOpen(true); // Abre el modal.
  };

  // Filtra los BLs de la naviera seleccionada.
  const selectedNavieraBLs = selectedNaviera
    ? bls.filter(bl => bl.naviera_id === selectedNaviera.id)
    : [];

  return (
    <div className="container mx-auto px-4 py-8">
      <div className="mb-8">
        <h1 className="text-3xl font-bold text-gray-900">Dashboard</h1>
        <p className="mt-2 text-gray-600">Monitoreo de BLs y estado del sistema</p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
        <StatCard
          title="Total BLs"
          value={totalBLs}
          icon={Package}
          trend="up"
          trendValue="+5% vs ayer"
        />
        <StatCard
          title="Pendientes de Revisión"
          value={pendingReview}
          icon={Clock}
          trend="down"
          trendValue="-2% vs ayer"
        />
        <StatCard
          title="Completados Hoy"
          value={completedToday}
          icon={CheckCircle2}
          trend="up"
          trendValue="+10% vs ayer"
        />
        <StatCard
          title="Con Errores"
          value={withErrors}
          icon={AlertTriangle}
          trend="down"
          trendValue="-3% vs ayer"
        />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-8">
        <div className="lg:col-span-2 bg-white rounded-lg shadow p-6">
          <h3 className="text-lg font-medium text-gray-900 mb-4">Estado por Naviera</h3>
          <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 gap-4">
            {NAVIERAS.map((naviera) => {
              const navieraBLs = bls.filter(bl => bl.naviera_id === naviera.id);
              const pendingCount = navieraBLs.filter(bl => !bl.revisado_con_exito).length;
              
              return (
                <button
                  key={naviera.id}
                  onClick={() => handleNavieraClick(naviera)}
                  className="p-4 bg-gray-50 rounded-lg hover:bg-gray-100 transition-colors"
                >
                  <div className="flex items-center justify-between mb-2">
                    <Ship className="w-5 h-5 text-blue-600" />
                    {pendingCount > 0 && (
                      <span className="bg-red-100 text-red-800 text-xs font-medium px-2 py-0.5 rounded-full">
                        {pendingCount}
                      </span>
                    )}
                  </div>
                  <h4 className="text-sm font-medium text-gray-900">{naviera.name}</h4>
                  <p className="text-xs text-gray-500 mt-1">
                    {navieraBLs.length} BLs totales
                  </p>
                </button>
              );
            })}
          </div>
        </div>

        <StatusBreakdown data={statusData} />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="bg-white rounded-lg shadow p-6">
          <h3 className="text-lg font-medium text-gray-900 mb-4">Progreso de Validación</h3>
          <div className="space-y-6">
            <ProgressBar
              label="POL Validados"
              value={75}
              max={100}
            />
            <ProgressBar
              label="POD Validados"
              value={82}
              max={100}
            />
            <ProgressBar
              label="Etapas de Transporte"
              value={60}
              max={100}
            />
          </div>
        </div>

        <div className="bg-white rounded-lg shadow p-6">
          <h3 className="text-lg font-medium text-gray-900 mb-4">Alertas Recientes</h3>
          <div className="space-y-4">
            {[
              { message: 'Datos incompletos en BL MAEU123456789', time: '5 min ago', type: 'warning' },
              { message: 'Error de validación en POD', time: '15 min ago', type: 'error' },
              { message: 'Actualización pendiente de estado', time: '1 hour ago', type: 'info' }
            ].map((alert, index) => (
              <div key={index} className="flex items-start space-x-3">
                <div className={`p-1.5 rounded-full ${
                  alert.type === 'error' ? 'bg-red-100' :
                  alert.type === 'warning' ? 'bg-yellow-100' : 'bg-blue-100'
                }`}>
                  {alert.type === 'error' ? (
                    <XCircle className="w-4 h-4 text-red-600" />
                  ) : alert.type === 'warning' ? (
                    <AlertTriangle className="w-4 h-4 text-yellow-600" />
                  ) : (
                    <AlertCircle className="w-4 h-4 text-blue-600" />
                  )}
                </div>
                <div>
                  <p className="text-sm text-gray-900">{alert.message}</p>
                  <p className="text-xs text-gray-500">{alert.time}</p>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>

      {selectedNaviera && (
        <NavieraModal
          isOpen={isModalOpen}
          onClose={() => setIsModalOpen(false)}
          naviera={selectedNaviera}
          bls={selectedNavieraBLs}
        />
      )}
    </div>
  );
}