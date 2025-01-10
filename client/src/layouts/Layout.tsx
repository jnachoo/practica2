import { Outlet } from "react-router-dom"; // Importa el componente Outlet para renderizar contenido de rutas hijas.
import { DefaultSidebar } from "../components/DefaultSidebar"; // Importa el componente de barra lateral predeterminado.

export default function Layout() {
  return (
    <div className="flex"> 
      {/* Contenedor principal que utiliza flexbox para organizar los elementos en fila */}
      
      {/* Sidebar */}
      <DefaultSidebar /> 
      {/* Renderiza la barra lateral (DefaultSidebar), que es común a todas las páginas */}
      
      {/* Main content */}
      <main className="flex-1 bg-neutral-200 p-5 overflow-y-auto h-screen">
        {/* flex-1: El contenido principal toma todo el espacio disponible.
            bg-neutral-200: Fondo de color neutral.
            p-5: Espaciado interno.
            overflow-y-auto: Habilita el scroll vertical si el contenido es demasiado largo.
            h-screen: Asegura que el contenedor tenga el 100% de la altura de la ventana. */}
        
        <Outlet />
        {/* Renderiza el contenido específico de la ruta hija actual */}
      </main>
    </div>
  );
}
