import React, { useState, useEffect } from "react";

interface BlData {
  bl_code: string;
  fecha_bl: string;
  estado: string;
}

const Bls: React.FC = () => {
  const [bls, setBls] = useState<BlData[]>([]);

  useEffect(() => {
    // Datos simulados
    const fetchData = async () => {
      try {
        const data: BlData[] = [
          { bl_code: "BL001", fecha_bl: "2025-01-01", estado: "Pendiente" },
          { bl_code: "BL002", fecha_bl: "2025-01-02", estado: "En proceso" },
          { bl_code: "BL003", fecha_bl: "2025-01-03", estado: "Completado" },
        ];
        // Simular un tiempo de espera como si fuera una API
        await new Promise((resolve) => setTimeout(resolve, 500));
        setBls(data);
      } catch (error) {
        console.error("Error fetching BLS data:", error);
      }
    };
    fetchData();
  }, []);

  return (
    <div className="p-6">
      <h1 className="text-2xl font-bold mt-10 mb-6">Lista de BLS (Ignorar Contenido)</h1>
      {/* Tabla de BLS */}
      <table className="min-w-full bg-white border border-gray-300">
        <thead>
          <tr>
            <th className="border px-4 py-2">Código BL</th>
            <th className="border px-4 py-2">Fecha</th>
            <th className="border px-4 py-2">Estado</th>
          </tr>
        </thead>
        <tbody>
          {bls.length > 0 ? (
            bls.map((bl) => (
              <tr key={bl.bl_code}>
                <td className="border px-4 py-2">{bl.bl_code}</td>
                <td className="border px-4 py-2">{bl.fecha_bl}</td>
                <td className="border px-4 py-2">{bl.estado}</td>
              </tr>
            ))
          ) : (
            <tr>
              <td
                colSpan={3}
                className="border px-4 py-2 text-center text-gray-500"
              >
                No hay datos disponibles.
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
};

export default Bls;
