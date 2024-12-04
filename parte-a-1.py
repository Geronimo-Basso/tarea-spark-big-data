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
    df = pd.read_csv('data/ratings.csv')
    times.append(['Carga de ratings.csv', round(time.time() - start, 2)])

    start = time.time()
    print(df.groupby(by=['rating']).size().sort_values(ascending=False))
    times.append(['Conteo valoraciones', round(time.time() - start, 2)])

    tabla_tiempos = pd.DataFrame(times, columns=['Operación', 'Tiempo (segundos)'])
    print(tabla_tiempos.to_string(index=False))
