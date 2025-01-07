# practica2
API desarrollada para Brains

Antes de ejecutar se sugiere crear un entorno en python e instalar las librerias usadas 
en dicho entorno

Una vez se hayan instalado las librerias se deberán configurar las credenciales de la base de 
datos, es decir se deberá cambiar el nombre de usuario. Además se debe estar conectado a la VPN
para que funcione el código.

Una vez se hayan hecho los pasos anteriores, se debe ejecutar el comando: uvicorn api:app --reload
para correr la API, se desplegará una ruta localhost para ir a la ruta de la API.

Para probar las solicitudes hechas se puede hacer en la ruta localhost/docs, otra manera de probar
es en el mismo navegador escribir los endpoints en la barra de busqueda, por ejemplo:
localhost/bls/2025
