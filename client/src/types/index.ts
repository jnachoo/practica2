// Common Types
export interface User {
    id: string;
    name: string;
    role: 'admin' | 'operator' | 'analyst' | 'general_manager' | 'limited_user';
  }
  
  export interface BL {

    id: string;
  
    bl_code: string;
  
    naviera_id: string;
  
    fecha_bl: Date | undefined;
  
    revisado_con_exito: boolean;
  
    etapa: string;
  
    nave: string;
  
    manual_pendiente: boolean;
  
    id_carga: string;
  
    no_revisar: boolean;
  
    state_code: number;
  
    html_descargado: boolean;
  
    proxima_revision: Date;
  
    revisado_hoy: boolean;
  
    mercado: string;
  
  }


  export interface DashboardStats {
    activeBLs: number;
    downloadsInProgress: number;
    pendingAlerts: number;
    pendingValidations: number;
  }
  
  export interface TrafficTrend {
    date: string;
    count: number;
  }
  
  export interface DownloadStatus {
    label: string;
    value: number;
  }
  
  export interface NavieraDistribution {
    naviera: string;
    count: number;
  }

  export interface Container {
    id: string;
    code: string;
    size: number;
    type: string;
    estado: string;
  }
  
  export interface ContainerViaje {
    id: string;
    container_id: string;
    bl_id: string;
    peso_kg: number;
    localidad_actual: string;
  }
  
  export interface Parada {
    id: string;
    locode: string;
    pais: string;
    lugar: string;
  }
  
  export interface Tracking {
    id: string;
    bl_id: string;
    fecha: Date;
    status: string;
    orden: number;
    parada_id: string;
    terminal: string;
    is_pol: boolean;
    is_pod: boolean;
  }
  
  export interface Request {
    id: string;
    bl_id: string;
    url: string;
    timestamp: Date;
    success: boolean;
    response_code: number;
    error: string;
    id_html: number;
  }