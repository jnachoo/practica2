import { useState } from "react";
import { Link } from "react-router-dom"; // Importar Link de react-router-dom

export function DefaultSidebar() {
  const [open, setOpen] = useState(true);
  const Menus = [
    { title: "Dashboard", src: "dashboard", route: "/" }, 
    { title: "Bls", src: "notepad", route: "/BLS" },
    { title: "PageExample", src: "loader", gap: true, route: "/PageExample" },
    { title: "PageExample", src: "loader", route: "/schedule" },
    { title: "PageExample", src: "loader", route: "/search" },
    { title: "PageExample", src: "loader", route: "/analytics" },
    { title: "PageExample", src: "loader", gap: true, route: "/files" },
    { title: "Configuracion", src: "cog", route: "/settings" },
  ];

  return (
    <div
      className={`${
        open ? "w-72" : "w-20"
      } bg-slate-100 h-screen p-2 pt-8 relative duration-300`}
    >
      <img
        src="./src/assets/control.png"
        className={`absolute cursor-pointer -right-3 top-3 w-7 border-dark-purple
           border-2 rounded-full  ${!open && "rotate-180"}`}
        onClick={() => setOpen(!open)}
      />
      <div className="flex gap-x-4 items-center">
        <img
          src="./src/assets/logo_Brains.svg"
          className={`cursor-pointer duration-500 ${open && "rotate-[360deg]"}`}
        />
        <h1
          className={`text-slate-950 origin-left font-extrabold text-xl duration-200 ${
            !open && "scale-0"
          }`}
          
        >Scraper</h1>
      </div>
      <ul className="pt-6">
        {Menus.map((Menu, index) => (
          <li
            key={index}
            className={`flex rounded-md p-3 font-bold cursor-pointer hover:bg-light-white text-slate-950 text-sm items-center gap-x-4 
              ${Menu.gap ? "mt-4" : "mt-4"} ${
              index === 0 && "bg-light-white"
            } ${!open ? "justify-center" : ""}`}
          >
            <img
              src={`./src/assets/${Menu.src}.png`}
              className={`transition-all duration-200 ${!open ? "scale-150" : "scale-150"}`}
            />
            <Link to={Menu.route} className={`${!open && "hidden"} origin-left duration-200`}>
              <span>{Menu.title}</span>
            </Link>
          </li>
        ))}
      </ul>
    </div>
  );
}
