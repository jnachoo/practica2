import React from 'react';
import ReactDOM from 'react-dom/client';
import { RouterProvider } from 'react-router-dom';
import { ThemeProvider } from '@material-tailwind/react'; // Asegúrate de que esto esté importado
import { router } from './router';

import './index.css';

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <ThemeProvider> {/* Envuelve tu app en ThemeProvider */}
      <RouterProvider router={router} />
    </ThemeProvider>
  </React.StrictMode>,
);
