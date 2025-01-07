
# Frontend desarrollado para Scraper de Brains

- Este proyecto Frontend esta en desarrollo y será una aplicación web integral para gestionar datos logísticos mediante un CRUD avanzado y un sistema de scraping manual y automatizado. La aplicación permitira recolectar, validar y unificar información clave, como BLs, contenedores y órdenes de descarga, presentándola en un dashboard informativo. Además, contara con roles y permisos definidos, validaciones de datos en tiempo real y restricciones de seguridad, como el uso exclusivo dentro de una VPN. Construida con React en el frontend y una API en Python para el backend, ofrece una solución escalable, segura y fácil de usar para optimizar operaciones logísticas.

## Tecnologías  

- **React** + **Vite**: Desarrollo rápido y moderno. 
- **Tailwind CSS**: Estilos rápidos y personalizables. 
- **TypeScript**: Tipado estático para mayor seguridad. 

## Instalación  

1. Clona el repositorio: 
   ```bash 
   git clone https://github.com/tu-usuario/tu-repositorio.git 
   cd tu-repositorio 
   ``` 

2. Instala las dependencias: 
   ```bash 
   npm install
   ``` 

3. Inicia el servidor de desarrollo: 
   ```bash 
   npm run dev 
   ``` 

## Scripts  

- **`npm run dev`**: Modo desarrollo. 
- **`npm run build`**: Compila para producción. 
- **`npm run preview`**: Previsualización del build. 

## Estructura  

```plaintext  
src/  
├── assets/           # Recursos estáticos como imágenes, fuentes y estilos personalizados.      
├── components/       # Componentes reutilizables de la interfaz de usuario (Sidebar,botones, tablas, etc.).      
├── Layouts/          # Estructuras de diseño globales como encabezados, pies de página o contenedores principales.      
├── pages/            # Páginas principales de la aplicación que representan vistas completas (e.g., Home, Dashboard).        
├── services/         # Funciones para interactuar con APIs o lógica de negocio específica.           
├── types/            # Tipos y definiciones para TypeScript usados en todo el proyecto.           
├── main.tsx          # Punto de entrada principal del proyecto donde se renderiza la aplicación.           
├── router.tsx        # Configuración de las rutas y navegación de la aplicación.          
├── index.css         # Estilos globales de la aplicación.          
     
```  

