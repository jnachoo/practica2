// Archivo: blServices.ts
// Este archivo contiene funciones para gestionar BLs (Bill of Lading) simulados y proporciona 
// la base para integrarlos con una API futura.

import { mockBLs, BL } from './mockBLs';
import { v4 as uuidv4 } from 'uuid';

// URL base para la API. Actualmente usa una variable de entorno o un valor predeterminado.
const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000/api';

// Variable en memoria que simula una base de datos local de BLs.
let inMemoryBLs = [...mockBLs];

/**
 * Simula la obtención de BLs desde una fuente, aplicando filtros si se proporcionan.
 * @param {Record<string, any>} filters - Filtros opcionales para filtrar los BLs.
 * @returns {Promise<BL[]>} Lista de BLs filtrados.
 */
export async function fetchBLs(filters?: Record<string, any>) {
  await new Promise((resolve) => setTimeout(resolve, 500)); // Simula un retraso en la red.

  let filteredBLs = [...inMemoryBLs];
  if (filters) {
    if (filters.bl_code) {
      filteredBLs = filteredBLs.filter((bl) => bl.bl_code.includes(filters.bl_code));
    }
    if (filters.naviera_id) {
      filteredBLs = filteredBLs.filter((bl) => bl.naviera_id === filters.naviera_id);
    }
    if (filters.startDate) {
      const startDate = new Date(filters.startDate);
      filteredBLs = filteredBLs.filter((bl) => bl.fecha_bl && new Date(bl.fecha_bl) >= startDate);
    }
    if (filters.endDate) {
      const endDate = new Date(filters.endDate);
      filteredBLs = filteredBLs.filter((bl) => bl.fecha_bl && new Date(bl.fecha_bl) <= endDate);
    }
  }
  return filteredBLs;
}

/**
 * Simula la creación de un nuevo BL mediante la API.
 * @param {Partial<BL>} data - Datos del BL a crear.
 * @returns {Promise<any>} Respuesta de la API.
 */
export async function createBL(data: Partial<BL>) {
  const response = await fetch(`${API_BASE_URL}/bls`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  });
  if (!response.ok) throw new Error('Failed to create BL');
  return response.json();
}

/**
 * Simula la actualización de un BL existente mediante la API.
 * @param {string} id - ID del BL a actualizar.
 * @param {Partial<BL>} data - Datos a actualizar.
 * @returns {Promise<any>} Respuesta de la API.
 */
export async function updateBL(id: string, data: Partial<BL>) {
  const response = await fetch(`${API_BASE_URL}/bls/${id}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  });
  if (!response.ok) throw new Error('Failed to update BL');
  return response.json();
}

/**
 * Simula la eliminación de un BL mediante la API.
 * @param {string} id - ID del BL a eliminar.
 * @returns {Promise<any>} Respuesta de la API.
 */
export async function deleteBL(id: string) {
  const response = await fetch(`${API_BASE_URL}/bls/${id}`, {
    method: 'DELETE',
  });
  if (!response.ok) throw new Error('Failed to delete BL');
  return response.json();
}

/**
 * Simula la importación de múltiples BLs desde un archivo CSV. 
 * Actualmente usa datos simulados en lugar de conectarse a una API real.
 * @param {Partial<BL>[]} data - Lista de datos parciales de BLs.
 * @returns {Promise<{ success: boolean; imported: number; total: number }>} Resumen del proceso de importación.
 */
export async function importBLsFromCSV(data: Partial<BL>[]) {
  // TODO: Reemplazar con la implementación real una vez la API esté disponible.
  // const response = await fetch(`${API_BASE_URL}/bls/import`, {
  //   method: 'POST',
  //   headers: { 'Content-Type': 'application/json' },
  //   body: JSON.stringify({ bls: data }),
  // });
  // if (!response.ok) throw new Error('Failed to import BLs');
  // return response.json();

  await new Promise((resolve) => setTimeout(resolve, 1000)); // Simula un retraso en la red.

  try {
    // Genera nuevos BLs completos a partir de los datos parciales.
    const newBLs: BL[] = data.map((partialBL) => ({
      id: uuidv4(), // Genera un ID único.
      bl_code: partialBL.bl_code || '',
      naviera_id: partialBL.naviera_id || '',
      fecha_bl: partialBL.fecha_bl ? new Date(partialBL.fecha_bl) : new Date(),
      revisado_con_exito: false,
      etapa: partialBL.etapa || 'Pendiente',
      nave: partialBL.nave || '',
      manual_pendiente: false,
      id_carga: `CARGO${Math.random().toString(36).substr(2, 9)}`,
      no_revisar: false,
      state_code: 1,
      html_descargado: false,
      proxima_revision: new Date(Date.now() + 24 * 60 * 60 * 1000), // Fecha de mañana.
      revisado_hoy: false,
      mercado: partialBL.mercado || 'asia',
    }));

    // Actualiza la lista en memoria con los nuevos BLs.
    inMemoryBLs = [...inMemoryBLs, ...newBLs];

    return {
      success: true,
      imported: newBLs.length,
      total: inMemoryBLs.length,
    };
  } catch (error) {
    console.error('Error importing BLs:', error);
    throw new Error('Failed to import BLs');
  }
}
