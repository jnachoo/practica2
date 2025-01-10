import React, { useState } from 'react';
import { X, Upload, AlertCircle } from 'lucide-react';
import Papa from 'papaparse';
import type { BL } from '../types';

// Definir la interfaz de las propiedades que recibirá el modal
interface ImportCSVModalProps {
  isOpen: boolean; // Indica si el modal está visible o no
  onClose: () => void; // Función de callback para cerrar el modal
  onImport: (data: Partial<BL>[]) => Promise<void>; // Función de callback para manejar los datos importados
}

// Componente principal para importar archivos CSV
export function ImportCSVModal({ isOpen, onClose, onImport }: ImportCSVModalProps) {
  // Estado para almacenar el archivo seleccionado
  const [file, setFile] = useState<File | null>(null);
  // Estado para almacenar mensajes de error
  const [error, setError] = useState<string>('');
  // Estado para indicar si se está procesando el archivo
  const [isLoading, setIsLoading] = useState(false);

  // Si el modal no está abierto, no renderiza nada
  if (!isOpen) return null;

  // Manejar el cambio de archivo seleccionado por el usuario
  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const selectedFile = e.target.files?.[0]; // Obtener el archivo seleccionado
    if (selectedFile && selectedFile.type === 'text/csv') { // Verificar si es un archivo CSV válido
      setFile(selectedFile); // Actualizar el estado del archivo
      setError(''); // Limpiar cualquier error existente
    } else {
      setError('Por favor selecciona un archivo CSV válido'); // Mostrar un mensaje de error para archivos no válidos
    }
  };

  // Manejar el proceso de importación del archivo CSV
  const handleImport = async () => {
    if (!file) return; // Si no hay un archivo seleccionado, salir de la función

    setIsLoading(true); // Indicar que se está procesando el archivo
    setError(''); // Limpiar cualquier error existente

    // Utilizar PapaParse para procesar el archivo CSV
    Papa.parse(file, {
      header: true, // Tratar la primera fila como encabezados
      skipEmptyLines: true, // Ignorar líneas vacías
      complete: async (results: Papa.ParseResult<any>) => {
        try {
          // Transformar y validar los datos parseados
          const transformedData = results.data.map((row: any) => ({
            bl_code: row.bl_code, // Extraer el código BL
            naviera_id: row.naviera_id, // Extraer el ID de la naviera
            fecha_bl: new Date(row.fecha_bl), // Convertir la fecha a un objeto Date
            etapa: row.etapa || 'Pendiente', // Etapa por defecto si no se proporciona
            nave: row.nave, // Extraer el nombre de la nave
            mercado: row.mercado, // Extraer el mercado
            // Valores por defecto para campos adicionales
            revisado_con_exito: false,
            manual_pendiente: false,
            no_revisar: false,
            html_descargado: false,
            revisado_hoy: false,
          }));

          await onImport(transformedData); // Llamar al callback de importación con los datos transformados
          onClose(); // Cerrar el modal
        } catch (err) {
          setError('Error al procesar el archivo. Verifica el formato de los datos.'); // Mostrar un error de procesamiento
        } finally {
          setIsLoading(false); // Restablecer el estado de carga
        }
      },
      error: () => {
        setError('Error al leer el archivo CSV'); // Mostrar un error al leer el archivo
        setIsLoading(false); // Restablecer el estado de carga
      },
    });
  };

  // Renderizar la interfaz del modal
  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 z-50 flex items-center justify-center">
      {/* Contenedor del modal */}
      <div className="bg-white rounded-lg shadow-xl w-full max-w-md">
        {/* Encabezado del modal */}
        <div className="p-4 border-b border-gray-200 flex items-center justify-between">
          <h2 className="text-xl font-semibold text-gray-900">
            Importar BLs desde CSV {/* Título del modal */}
          </h2>
          <button
            onClick={onClose} // Cierra el modal al hacer clic en el botón
            className="p-1 hover:bg-gray-100 rounded-full"
          >
            <X className="w-5 h-5 text-gray-500" /> {/* Ícono para cerrar */}
          </button>
        </div>

        {/* Cuerpo del modal */}
        <div className="p-6">
          {/* Sección para subir el archivo */}
          <div className="mb-6">
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Archivo CSV {/* Etiqueta para el input del archivo */}
            </label>
            <div className="mt-1 flex justify-center px-6 pt-5 pb-6 border-2 border-gray-300 border-dashed rounded-md">
              <div className="space-y-1 text-center">
                <Upload className="mx-auto h-12 w-12 text-gray-400" /> {/* Ícono de subida */}
                <div className="flex text-sm text-gray-600">
                  <label className="relative cursor-pointer bg-white rounded-md font-medium text-blue-600 hover:text-blue-500">
                    <span>Seleccionar archivo</span> {/* Texto del botón para seleccionar archivo */}
                    <p className="text-xs text-gray-500">Formato CSV Ejemplo bl_code,naviera_id,fecha_bl,etapa,nave,mercado</p> {/* Indicación del tamaño máximo permitido */}
                    <input
                      type="file" // Input para seleccionar archivos
                      className="sr-only"
                      accept=".csv" // Acepta solo archivos CSV
                      onChange={handleFileChange} // Llama a handleFileChange al seleccionar un archivo
                    />
                  </label>
                </div>
                <p className="text-xs text-gray-500">CSV hasta 10MB</p> {/* Indicación del tamaño máximo permitido */}
                
              </div>
            </div>
            {file && (
              <p className="mt-2 text-sm text-gray-600">
                Archivo seleccionado: {file.name} {/* Muestra el nombre del archivo seleccionado */}
              </p>
            )}
          </div>

          {/* Mostrar mensaje de error */}
          {error && (
            <div className="mb-4 p-3 bg-red-50 rounded-md flex items-start space-x-2">
              <AlertCircle className="w-5 h-5 text-red-400 mt-0.5" /> {/* Ícono de error */}
              <p className="text-sm text-red-600">{error}</p> {/* Muestra el mensaje de error */}
            </div>
          )}

          {/* Botones de acción */}
          <div className="mt-6 flex justify-end space-x-3">
            <button
              onClick={onClose} // Cierra el modal al hacer clic en el botón
              className="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50"
            >
              Cancelar {/* Botón para cancelar */}
            </button>
            <button
              onClick={handleImport} // Llama a handleImport al hacer clic en el botón
              disabled={!file || isLoading} // Deshabilita el botón si no hay archivo o está cargando
              className={`px-4 py-2 text-sm font-medium text-white rounded-md ${
                !file || isLoading
                  ? 'bg-blue-400 cursor-not-allowed' // Estilo para botón deshabilitado
                  : 'bg-blue-600 hover:bg-blue-700' // Estilo para botón habilitado
              }`}
            >
              {isLoading ? 'Importando...' : 'Importar'} {/* Muestra texto de carga o importar */}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
