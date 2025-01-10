// Importamos el tipo LucideIcon desde la librería lucide-react para usar íconos en el componente
import { LucideIcon } from 'lucide-react';

// Definimos la interfaz para las propiedades que acepta el componente
interface StatCardProps {
  title: string;        // Título de la tarjeta
  value: number;        // Valor principal que se mostrará
  icon: LucideIcon;     // Ícono que se mostrará en la tarjeta
  trend?: 'up' | 'down'; // Indica si hay una tendencia al alza ('up') o a la baja ('down')
  trendValue?: string;  // Valor adicional que describe la tendencia (por ejemplo, "+10%")
}

// Componente funcional StatCard
export function StatCard({ title, value, icon: Icon, trend, trendValue }: StatCardProps) {
  return (
    <div className="bg-white rounded-lg shadow p-6">
      {/* Contenedor superior con el ícono, título y tendencia */}
      <div className="flex items-center justify-between">
        {/* Sección con el ícono y el título */}
        <div className="flex items-center space-x-3">
          {/* Contenedor para el ícono */}
          <div className="p-2 bg-blue-50 rounded-lg">
            <Icon className="w-6 h-6 text-blue-600" /> {/* Ícono proporcionado como prop */}
          </div>
          <h3 className="text-sm font-medium text-gray-900">{title}</h3> {/* Título */}
        </div>

        {/* Mostrar información de tendencia si se proporciona */}
        {trend && (
          <span
            className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${
              trend === 'up' 
                ? 'bg-red-100 text-red-800' // Estilo para tendencia al alza
                : 'bg-green-100 text-green-800' // Estilo para tendencia a la baja
            }`}
          >
            {trendValue} {/* Valor de la tendencia (por ejemplo, "+10%" o "-5%") */}
          </span>
        )}
      </div>

      {/* Sección con el valor principal */}
      <p className="mt-4 text-2xl font-semibold text-gray-900">{value}</p> {/* Valor principal */}
    </div>
  );
}
