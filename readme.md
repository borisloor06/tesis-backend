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

```bash
flask --app server.py --debug run 
```
## Lista Endpoints funcionales

### get subreddit sin api
> localhost:5000/reddit
>
> localhost:5000/reddit?subreddit=ChatGpt&listing=hot&limit=100&timeframe=all

- subreddit -> Nombre del subreddit a consultar
- listing -> Tipo de listado a consultar (controversial, best, hot, new, random, rising, top)
- limit -> Cantidad de resultados a obtener
- timeframe -> Periodo de tiempo a consultar (hour, day, week, month, year, all)

### get subreddit con appi
se ejecutara indefinidamente mientras tenga la petición activa

> localhost:5000/subreddit
>
> localhost:5000/subreddit?name=ChatGpt

- name -> Nombre del subreddit a consultar by default is ChatGPT
get all subreddit of r/ChatGPT reddit

### get subredit by limit
se ejecutara en segundo plano, dependera de la potencia del servidor

> localhost:5000/subreddit_by_limit?name=ChatGpt&limit=10&time_filter=day

- name -> Nombre del subreddit a consultar by default is ChatGPT
- limit -> Limite de posts a consultar by default is None 
- time_filter -> Tiempo de post a consultar by default is day 
  disponibles
    - all
    - day
    - hour
    - month
    - week
    - year

### realizar el analisis
este endpoint obtendra toda la información guardada de un subreddit en la base de datos
limpiara la información y ejecutara todos los analisis preprogramados, 
se recomienda ejecutarlo de manera indivual controlando su ejecución porque podría consumir todos los 
recursos de su maquina

> localhost:5000/make_analisis?name=ChatGpt
- name -> Nombre del subreddit a consultar by default is ChatGPT



### get analisis data and reddit data
> localhost:5000/analisis
>
> localhost:5000/analisis?name=ChatGpt

- name -> Nombre del subreddit a consultar by default is ChatGPT