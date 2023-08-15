# Crear entorno virtual
usar virtualenv y virtualenvwrapper
## Instalar dependencias del requirements.txt
```bash
pip install -r requirements.txt
```
# Obtener refresh token reddit
Crea un archivo praw.ini en base al archivo de ejemplo y completa los datos de la app de reddit, luego ejecuta el script
```bash
python refreshToken.py
```
Agrega el token al archivo praw.ini



# Ejecutar el servidor
Una vez completado los pasos anteriores puedes ejecutar el servidor
```bash
python server.py
```

## Lista Endpoints funcionales

### get subreddit sin api
> localhost:5000/reddit
>
> localhost:5000/reddit?subreddit=ChatGPT&listing=hot&limit=100&timeframe=all

- subreddit -> Nombre del subreddit a consultar
- listing -> Tipo de listado a consultar (controversial, best, hot, new, random, rising, top)
- limit -> Cantidad de resultados a obtener
- timeframe -> Periodo de tiempo a consultar (hour, day, week, month, year, all)

### get subreddit con appi

> localhost:5000/subreddit
get all subreddit of r/ChatGPT reddit
