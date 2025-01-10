import { useState } from "react"; // Hook para manejar el estado.
import { Link, useLocation } from "react-router-dom"; // Herramientas para navegación y detección de la ruta activa.

export function DefaultSidebar() {
  const [open, setOpen] = useState(true); 
  // Estado local para manejar si la barra lateral está expandida (true) o contraída (false).

  const location = useLocation(); 
  // Hook de react-router-dom para obtener la información de la ruta actual.

  const Menus = [
    { title: "Dashboard", src: "dashboard", route: "/" },
    { title: "BLs", src: "notepad", route: "/BLS" },
    { title: "PageExample", src: "loader", gap: true, route: "/PageExample" },
    { title: "Configuracion (aun no implementado)", src: "cog", route: "/Configuracion" },
  ]; 
  // Array de objetos para definir los elementos del menú. Cada objeto contiene:
  // - `title`: El nombre que se muestra.
  // - `src`: El nombre del icono (ruta relativa a los assets).
  // - `route`: Ruta asociada al enlace.
  // - `gap` (opcional): Define si debe haber un espacio adicional antes del ítem.

  return (
    <div
      className={`${
        open ? "w-72" : "w-20"
      } bg-slate-100 h-screen p-2 pt-8 relative duration-300`}
    >
      {/* Contenedor principal de la barra lateral:
          - `w-72` cuando está expandida.
          - `w-20` cuando está contraída.
          - `duration-300`: Transición suave entre estados. */}
      
      {/* Botón de control para expandir/contraer */}
      <img
        src="./src/assets/control.png"
        className={`absolute cursor-pointer -right-3 top-3 w-7 border-dark-purple
           border-2 rounded-full ${!open && "rotate-180"}`}
        onClick={() => setOpen(!open)} // Alterna entre expandir y contraer.
      />
      
      {/* Logo y título */}
      <div className="flex gap-x-4 items-center">
        <img
          src="./src/assets/logo_Brains.svg"
          className={`cursor-pointer duration-500 ${open && "rotate-[360deg]"}`}
        />
        <h1
          className={`text-transparent bg-clip-text bg-gradient-to-r to-emerald-600 from-sky-400 origin-left font-extrabold text-3xl duration-200 ${
            !open && "scale-0"
          }`}
        >
          Web Scraper
        </h1>
      </div>

      {/* Lista de menús */}
      <ul className="pt-6">
        {Menus.map((Menu, index) => (
          <li
            key={index}
            className={`flex rounded-md p-3 font-bold cursor-pointer ${
              location.pathname === Menu.route ? "bg-indigo-950 text-white" : "hover:bg-indigo-950 hover:text-white text-blue-950"
            } text-sm items-center gap-x-4 
              ${Menu.gap ? "mt-4" : "mt-4"} ${!open ? "justify-center" : ""}`}
          >
            <Link
              to={Menu.route} // Navegación hacia la ruta correspondiente.
              className="flex items-center gap-x-4 w-full"
            >
              {/* Icono del menú */}
              <img
                src={`./src/assets/${Menu.src}.png`}
                className={`transition-all duration-200 ${
                  !open ? "scale-150" : "scale-150"
                }`}
              />
              {/* Texto del menú */}
              <span
                className={`${
                  !open ? "hidden" : ""
                } origin-left duration-200`}
              >
                {Menu.title}
              </span>
            </Link>
          </li>
        ))}
      </ul>
    </div>
  );
}
