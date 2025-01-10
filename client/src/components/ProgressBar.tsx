// Definimos la interfaz para las propiedades que acepta el componente
interface ProgressBarProps {
    value: number; // Valor actual del progreso
    max: number;   // Valor máximo que se puede alcanzar
    label: string; // Etiqueta que describe el progreso
  }
  
  // Componente funcional ProgressBar
  export function ProgressBar({ value, max, label }: ProgressBarProps) {
    // Calculamos el porcentaje redondeando el resultado de (valor actual / valor máximo) * 100
    const percentage = Math.round((value / max) * 100);
  
    // Retornamos el JSX del componente
    return (
      <div className="mb-4">
        {/* Encabezado de la barra de progreso: muestra la etiqueta y el porcentaje */}
        <div className="flex justify-between mb-1">
          <span className="text-sm font-medium text-gray-700">{label}</span> {/* Etiqueta descriptiva */}
          <span className="text-sm font-medium text-gray-700">{percentage}%</span> {/* Porcentaje */}
        </div>
  
        {/* Contenedor de la barra de progreso */}
        <div className="w-full bg-gray-200 rounded-full h-2.5">
          {/* Indicador de progreso, se llena según el porcentaje calculado */}
          <div
            className="bg-blue-600 h-2.5 rounded-full"
            style={{ width: `${percentage}%` }} // Establecemos el ancho según el porcentaje
          ></div>
        </div>
      </div>
    );
  }
  