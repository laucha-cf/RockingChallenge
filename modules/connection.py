from sqlalchemy import create_engine, Column, Integer, Date, DateTime, String, ForeignKey
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import declarative_base
import psycopg2 as pg2
import logging


def connect_with_database( conn, cur, host, db, user, password ):
    #Try to make the connection with the Database (Postgres)
    try:
        conn = pg2.connect(f'host={host} dbname={db} user={user} password={password}')

        #Use the connection to get a cursor that can be used to execute queries
        cur = conn.cursor()

        # Set automatic commit to be true so that each action is commited without 
        # having to call conn.commit() after each command
        conn.set_session(autocommit=True)

        logging.info('Conexión establecida con base de datos {}.'.format(db))
    except pg2.Error as e:
        print(f'Error: Could not make connection to the {db} Database')
        print(e)

    return conn, cur

def close_connection( conn ):
    #Close the connection with database
    try:
        conn.close()
        logging.info('Conexión cerrada.')
    except pg2.Error as e:
        print('Could not close connection')
        print(e)

def create_database( cur, dbname ):

    # Create a Database
    try:
        cur.execute(f'CREATE DATABASE {dbname}')
        logging.info('Base de datos {} creada correctamente.'.format(dbname))
    except pg2.Error as e:
        print('Could not create the database because of the following error')
        print(e)

def create_tables(dbms, user, password, host, port, db_name):
    """Crea las tablas
    
    params:
        dbms: Motor de base de datos
        user: Usuario
        password: Contraseña
        host: Host
        port: Puerto
        db_name: Nombre de la base de datos
    return:
        None
    """
    #Creamos el engine y la base de datos
    engine = create_engine(f'{dbms}://{user}:{password}@{host}:{port}/{db_name}')
    
    #Se utiliza para declarar y crear las tablas
    Base = declarative_base()
    
    class ShowType(Base):
        __tablename__='show_type'
        type = Column(String(20))
        type_id = Column(Integer, primary_key=True)

    #Tabla master
    class Master(Base):
        __tablename__ = 'master_table'
        id = Column(Integer, primary_key=True)
        type_id = Column(Integer, ForeignKey('show_type.type_id'))
        title = Column(String(100), default='No Title')
        director = Column(String(100), default='No Director')
        cast = Column(String(100), default='No Cast')
        country = Column(String(50), default='No Country')
        date_added = Column(Date)
        release_year = Column(Integer, default=0)
        rating = Column(String(6), default='No Rating')
        duration = Column(String(6), default='No Duration')
        listed_id = Column(String(50), default='No Listed')
        description = Column(String(100), default='No Description')
        source = Column(String(25))
        
    Base.metadata.create_all(engine)
    
    logging.info('Tablas creadas correctamente.')
    
    #Cerramos la conexión
    engine.dispose()

def fill_table(dataframe, name, engine):
    """Puebla una tabla
    
    params:
        dataframe: DataFrame a utilizar para poblar.
    return:
        None
    """
    dataframe.to_sql(name, engine, if_exists='replace', index=False, method='multi')
    logging.info('Tabla {} cargada correctamente.'.format(name))

def load_db(to_load, list_names, dbms, user, password, host, port, db_name):
    """Carga las tablas de la base de datos en este orden
        Netflix, Disney, ShowType
    
    params:
        to_load: Lista con dataframes a cargar
        dbms: Motor de base de datos
        user: Usuario
        password: Contraseña
        host: Host
        port: Puerto
        db_name: Nombre de la base de datos
    return:
        None
    """
    #Genera la conexión con la base de datos
    engine = create_engine(f'{dbms}://{user}:{password}@{host}:{port}/{db_name}')
    connection = engine.connect()
    
    #Lista con los nombres de las tablas a poblar
    list_dataframes = to_load
    
    for i, name in enumerate(list_names):
        fill_table(list_dataframes[i], name, engine)
    
    #Cerramos la conexión
    connection.close()
    engine.dispose()