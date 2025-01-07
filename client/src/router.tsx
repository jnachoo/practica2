import { createBrowserRouter } from 'react-router-dom';
import Layout from './layouts/Layout';  // Layout común para todas las rutas
import Bls from './pages/Bls';  // Página Bls
// import Dashboard from './pages/Dashboard';  // Página Dashboard
import PageExample from './pages/PageExample';  // Página PageExample
// import Accounts from './pages/Accounts';  // Página Accounts
// import Settings from './pages/Settings';  // Página Settings

export const router = createBrowserRouter([
    {
        path: '/',
        element: <Layout />,  // El layout se aplica a todas las rutas hijas
        children: [
            // {   
            //     index: true,  // Esta es la ruta principal (cuando el path es '/')
            //     element: <Dashboard />  // Se renderiza la página Dashboard por defecto
            // },
            {
                path: '/bls',  // Ruta para la página Bls
                element: <Bls />
            },
            {
                path: '/PageExample',  // Ruta para la página Accounts
                element: <PageExample />
            },
            // {
            //     path: '/settings',  // Ruta para la página Settings
            //     element: <Settings />
            // }
        ]
    }
]);
