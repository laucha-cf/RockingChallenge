from modules.download_files_s3 import get_bucket_contents, download_bucket_files
from modules.connection import connect_with_database, create_database, close_connection
from modules.connection import create_tables, load_db
from modules.etl import etl_disney, etl_netflix
from dotenv import load_dotenv
import pandas as pd
import logging
import boto3
import os

#Cargamos nuestras variables de entorno
load_dotenv()
DBMS    = os.getenv('DBMS')
DB_NAME = os.getenv('DB_NAME')
USER    = os.getenv('USER')
HOST    = os.getenv('HOST')
DRIVER  = os.getenv('DRIVER')
PASSWORD= os.getenv('PASSWORD')
PORT    = os.getenv('PORT')

#Cargamos las credenciales
ACCES_KEY        = os.getenv('ACCES_KEY')
SECRET_ACCES_KEY = os.getenv('SECRET_ACCES_KEY')
BUCKET_NAME      = os.getenv('BUCKET_NAME')

#Establecemos el formato para los loggings
logging.FileHandler(filename='app_logs.log', mode='w')
logging.basicConfig(filename='app_logs.log',
                    level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

#Nuestro primer paso es interactuar con la API para acceder a los recursos
session = boto3.Session(aws_access_key_id = ACCES_KEY, 
                        aws_secret_access_key = SECRET_ACCES_KEY )
s3 = session.resource('s3')
bucket = s3.Bucket(BUCKET_NAME)

conn = None
cur = None

if __name__ == '__main__':
    #Obtenemos los archivos a descargar y luego los descargamos
    file_names = get_bucket_contents(bucket=bucket)
    download_bucket_files( s3, BUCKET_NAME, file_names, dir='raw_data')

    #Obtenemos los dataframes luego del proceso ETL
    df_netflix, df_show_types = etl_netflix( 'raw_data/netflix_titles.csv' )
    df_disney = etl_disney( 'raw_data/disney_plus_titles.csv' )
    df_master = pd.concat([df_disney, df_netflix])

    #Conexión con BDD por defecto
    conn, cur = connect_with_database( conn, cur, HOST, 'postgres', USER, PASSWORD )
    create_database( cur, DB_NAME )
    close_connection( conn )

    #Conexión con BDD RockingChallenge y creación de tablas
    create_tables( DBMS, USER, PASSWORD, HOST, PORT, DB_NAME )

    #Cargamos las tablas con sus respectivos CSVs
    load_db( [df_master, df_show_types],
             ['master_table', 'show_type'],
             DBMS, USER, PASSWORD, HOST, PORT, DB_NAME )
