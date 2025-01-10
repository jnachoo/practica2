import React from 'react';
import ReactDOM from 'react-dom/client'; // Importa el método para renderizar la aplicación en el DOM.
import { RouterProvider } from 'react-router-dom'; // Proveedor de rutas para manejar la navegación.
import { ThemeProvider } from '@material-tailwind/react'; // Proveedor de tema de TailwindCSS para gestionar estilos globales.
import { router } from './router'; // Importa el router configurado en el archivo router.tsx.

import './index.css'; // Importa los estilos globales de la aplicación.

ReactDOM.createRoot(document.getElementById('root')!).render( // Monta la aplicación en el elemento raíz del HTML.
  <React.StrictMode>
    {/* Modo estricto de React para detectar problemas potenciales en la aplicación */}
    <ThemeProvider> 
      {/* Proporciona los estilos y temas predefinidos de @material-tailwind/react a toda la aplicación */}
      <RouterProvider router={router} />
      {/* Proporciona la configuración de rutas definida en el archivo router.tsx */}
    </ThemeProvider>
  </React.StrictMode>,
);
