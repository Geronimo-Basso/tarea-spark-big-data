"""
Autores de este código:
MOHAMMED AZZOUZ (22153801)
GERÓNIMO BASSO SOSA (221A5673)
JORGE JAVIER CASTILLA COELLO (22000015)
GRANDES VOLÚMENES DE DATOS
"""
import time
import pandas as pd
import multiprocessing
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, asc

if __name__ == '__main__':
    times = []

    cpu_cores = multiprocessing.cpu_count()
    print(f"Using {cpu_cores} CPU cores.")

    spark = SparkSession.builder.master(f"local[{cpu_cores}]").appName("MoviesRatings").getOrCreate()

    start = time.time()
    movies = spark.read.csv(
        path='data/movies.csv',
        header=True,
        inferSchema=True,
        sep=',',
        encoding='UTF-8',
    )
    movies.count()
    times.append(['Carga de movies.csv', round(time.time() - start, 2)])

    start = time.time()
    ratings = spark.read.csv(
        path='data/ratings.csv',
        header=True,
        inferSchema=True,
        sep=',',
        encoding='UTF-8',
    )
    ratings.count()
    times.append(['Carga de ratings.csv', round(time.time() - start, 2)])

    start = time.time()
    average_ratings = ratings.groupBy('movieId').avg('rating').withColumnRenamed('avg(rating)', 'average_rating')
    movies_with_ratings = movies.join(average_ratings, on='movieId', how='left').sort(asc('movieId'))
    movies_with_ratings.show()
    times.append(['Combinación de datos', round(time.time() - start, 2)])

    movies_with_ratings.coalesce(1).write.csv('data/movies_rating', header=True) # Coalesce 1 para que se guarde completo en el mismo archivo
    times.append(['Guardar csv', round(time.time() - start, 2)])

    tabla_tiempos = pd.DataFrame(times, columns=['Operación', 'Tiempo (segundos)'])
    print(tabla_tiempos.to_string(index=False))

    spark.stop()
