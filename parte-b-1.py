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

if __name__ == '__main__':
    times = []

    cpu_cores = multiprocessing.cpu_count()
    print(f"Using {cpu_cores} CPU cores.")

    spark = SparkSession.builder.master(f"local[{cpu_cores}]").appName("Reviews").getOrCreate()

    start = time.time()
    df = spark.read.csv(
        path='data/ratings.csv',
        header=True,
        inferSchema=True,
        sep=',',
        encoding='UTF-8',
    )
    df.count()
    times.append(['Carga de ratings.csv', round(time.time() - start, 2)])

    start = time.time()
    df_rating = df.groupBy('rating').count().orderBy('count', ascending=False)
    df_rating.show()
    times.append(['Conteo valoraciones', round(time.time() - start, 2)])

    tabla_tiempos = pd.DataFrame(times, columns=['Operación', 'Tiempo (segundos)'])
    print(tabla_tiempos.to_string(index=False))

    spark.stop()
