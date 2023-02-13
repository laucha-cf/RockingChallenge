import pandas as pd
import numpy as np
import logging as log

def etl_disney( path ):
    """Realiza la limpieza de datos y separación de tablas
    para el CSV 'disney_plus_titles.csv'

    Params
        None
    Return
        df_disney: Dataframe principal.
    """
    df_disney = pd.read_csv(path)

    #Split en los siguientes campos
    df_disney['cast'] =      df_disney['cast'].str.split(', ').tolist()
    df_disney['country'] =   df_disney['country'].str.split(', ').tolist()
    df_disney['listed_in'] = df_disney['listed_in'].str.split(', ').tolist()

    #Reemplazamos null por valores default
    df_disney['title'] =        df_disney['title'].fillna(value='No Title')
    df_disney['director'] =     df_disney['director'].fillna(value='No Director')
    df_disney['cast'] =         df_disney['cast'].fillna(value='No Cast')
    df_disney['country'] =      df_disney['country'].fillna(value='No Country')
    df_disney['release_year'] = df_disney['release_year'].fillna(value=0)
    df_disney['rating'] =       df_disney['rating'].fillna(value='No Rating')
    df_disney['duration'] =     df_disney['duration'].fillna(value='No Duration')
    df_disney['listed_in'] =    df_disney['listed_in'].fillna(value='No Listed')

    #Creamos una nueva columna llamada 'id' y eliminamos 'show_id'
    df_disney.insert(loc=0, column='id', value=range(len(df_disney)))
    df_disney.drop('show_id', inplace=True, axis=1)

    #Creamos una nueva columna seteada en 0 por default
    df_disney.insert(loc=1, column='type_id', value=0 )

    #Reemplazamos cada type por un valor entero asociado y eliminamos 'type'
    df_disney['type_id'] = df_disney['type'].replace({'Movie':1, 'TV Show':2})
    df_disney.drop('type', inplace=True, axis=1)

    #Reemplazamos los nulos con el valor más frecuente
    most_frequent_date = df_disney['date_added'].value_counts().index[0]
    df_disney[df_disney['date_added'].str.strip()==''] = most_frequent_date
    
    #Reemplazamos la coma por vacío
    df_disney['date_added'] = df_disney['date_added'].str.replace(',', '')

    #Convertimos a tipo datetime
    df_disney['date_added'] = pd.to_datetime(df_disney['date_added'], format='%B %d %Y')

    df_disney['source'] = 'Disney'

    return df_disney


def etl_netflix( path ):
    """Realiza la limpieza de datos y separación de tablas
    para el CSV 'netflix_titles.csv'

    Params
        None
    Return
        df_netflix: Dataframe principal.
        df_type: Dataframe con la descripción del tipo de show. La misma
                 tambien puede ser utilizada para el dataframe df_disney.
    """
    df_netflix = pd.read_csv( path, sep=';' )

    #Split en los siguientes campos
    df_netflix['cast'] = df_netflix['cast'].str.split(', ').tolist()
    df_netflix['country'] = df_netflix['country'].str.split(', ').tolist()
    df_netflix['listed_in'] = df_netflix['listed_in'].str.split(', ').tolist()

    #Reemplazamos null por valores default
    df_netflix['title'] = df_netflix['title'].fillna(value='No Title')
    df_netflix['director'] = df_netflix['director'].fillna(value='No Director')
    df_netflix['cast'] = df_netflix['cast'].fillna(value='No Cast')
    df_netflix['country'] = df_netflix['country'].fillna(value='No Country')
    df_netflix['release_year'] = df_netflix['release_year'].fillna(value=0)
    df_netflix['rating'] = df_netflix['rating'].fillna(value='No Rating')
    df_netflix['duration'] = df_netflix['duration'].fillna(value='No Duration')
    df_netflix['listed_in'] = df_netflix['listed_in'].fillna(value='No Listed')

    #Creamos una nueva columna llamada 'id' y eliminamos 'show_id'
    df_netflix.insert(loc=0, column='id', value=range(len(df_netflix)))
    df_netflix.drop('show_id', inplace=True, axis=1)

    #Cambiamos el tipo de dato de release_year
    df_netflix['release_year'] = df_netflix['release_year'].replace(np.nan, '0')
    df_netflix['release_year'] = df_netflix['release_year'].replace('40 min', '0')

    #Ahora si podemos realizar la conversión
    df_netflix = df_netflix.astype({'release_year':'int'})

    #Existen valores nulos (NaN) y erróneos ('William Wyler'). Los reemplazamos por 'No Type'
    df_netflix['type'].fillna(value='No Type', inplace=True)
    df_netflix['type'] = df_netflix['type'].str.replace('William Wyler', 'No Type')

    #Creamos un nuevo dataframe con las descripciones de los tipos y sus id
    df_type = pd.DataFrame({
        'type': df_netflix['type'].unique()
        })
    df_type['type_id'] = df_type['type'].replace({'Movie':1, 'TV Show':2, 'No Type':0})

    #Creamos una nueva columna seteada en 0 por default
    df_netflix.insert(loc=1, column='type_id', value=0 )

    #Reemplazamos cada type por un valor entero asociado y eliminamos 'type'
    df_netflix['type_id'] = df_netflix['type'].replace({'Movie':1, 'TV Show':2})
    df_netflix.drop('type', inplace=True, axis=1)

    #Reemplazamos la coma por vacío y valores erroneos por vacío
    df_netflix['date_added'] = df_netflix['date_added'].str.replace(',', '')
    df_netflix['date_added'] = df_netflix['date_added'].str.replace('TV-PG', '')

    #Eliminamos los espacios sobrantes
    df_netflix['date_added'] = df_netflix['date_added'].str.strip()

    #Reemplazamos los nulos con el valor más frecuente
    most_frequent_date = df_netflix['date_added'].value_counts().index[0]
    df_netflix[df_netflix['date_added'].str.strip()==''] = most_frequent_date

    #Convertimos a tipo datetime
    df_netflix['date_added'] = pd.to_datetime(df_netflix['date_added'], format='%B %d %Y')

    df_netflix['source'] = 'Netflix'

    return df_netflix, df_type