"""
Autores de este código:
MOHAMMED AZZOUZ (22153801)
GERÓNIMO BASSO SOSA (221A5673)
JORGE JAVIER CASTILLA COELLO (22000015)
GRANDES VOLÚMENES DE DATOS
"""
import pandas as pd
import time

if __name__ == '__main__':
    times = []

    start = time.time()
    movies = pd.read_csv('data/movies.csv')
    times.append(['Carga de movies.csv', round(time.time() - start, 2)])

    start = time.time()
    ratings = pd.read_csv('data/ratings.csv')
    times.append(['Carga de ratings.csv', round(time.time() - start, 2)])

    start = time.time()
    average_ratings = ratings.groupby(by='movieId')['rating']
    average_ratings = average_ratings.mean().reset_index()
    average_ratings.rename(columns={'rating': 'average_rating'}, inplace=True)
    movies_with_ratings = movies.merge(average_ratings, on='movieId', how='left')
    times.append(['Combinación de datos', round(time.time() - start, 2)])

    start = time.time()
    movies_with_ratings.to_csv('data/movies_ratings.csv', index=False)
    times.append(['Guardar movies_rating.csv', round(time.time() - start, 2)])

    tabla_tiempos = pd.DataFrame(times, columns=['Operación', 'Tiempo (segundos)'])
    print(tabla_tiempos.to_string(index=False))
