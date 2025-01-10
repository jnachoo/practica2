import { createBrowserRouter } from 'react-router-dom'; // Crea un enrutador basado en el historial del navegador.
import Layout from './layouts/Layout'; // Importa el componente Layout que actúa como plantilla común.
import { BLsPage } from './pages/BLs'; // Página que lista los BLs.
import { Dashboard } from './pages/Dashboard'; // Página principal del dashboard.
import PageExample from './pages/PageExample'; // Página de ejemplo.
// import Accounts from './pages/Accounts'; // Ruta comentada que podría usarse para cuentas.
import Configuracion from './pages/Config'; // Ruta comentada que podría usarse para configuración.

export const router = createBrowserRouter([
    {
        path: '/', // Define la ruta base.
        element: <Layout />, // El componente Layout se aplica como plantilla para las rutas hijas.
        children: [
            {   
                index: true, // Ruta predeterminada cuando el usuario visita '/'.
                element: <Dashboard /> // Renderiza el componente Dashboard.
            },
            {
                path: '/bls', // Ruta para la página de BLs.
                element: <BLsPage /> // Renderiza el componente BLsPage.
            },
            {
                path: '/PageExample', // Ruta para la página de ejemplo.
                element: <PageExample /> // Renderiza el componente PageExample.
            },
            {
                path: '/Configuracion', // Ruta para configuración (comentada por ahora).
                element: <Configuracion /> 
            }
        ]
    }
]);
