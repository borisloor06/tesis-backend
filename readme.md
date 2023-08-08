# Crear entorno virtual
usar virtualenv y virtualenvwrapper

## Lista Endpoints funcionales
> localhost:5000/tweets?topic=chatgpt&limit=100
topic -> se refiere a cualquier tema a analizar
limit -> el número de tweets a retornar

> localhost:5000/reddit?subreddit=ChatGPT&listing=hot&limit=100&timeframe=all
> localhost:5000/reddit
subreddit -> Nombre del subreddit a consultar
listing -> Tipo de listado a consultar (controversial, best, hot, new, random, rising, top)
limit -> Cantidad de resultados a obtener
timeframe -> Periodo de tiempo a consultar (hour, day, week, month, year, all)