import { Outlet } from "react-router-dom";
import { DefaultSidebar } from "../components/DefaultSidebar";  // Importa el DefaultSidebar

export default function Layout() {
  return (
    <div className="flex">
      {/* Sidebar */}
      <DefaultSidebar /> 

      {/* Main content */}
      <main className="flex-1 bg-neutral-200 p-5 overflow-y-auto h-screen">
        <Outlet />
      </main>
    </div>
  );
}
