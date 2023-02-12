from botocore.exceptions import ClientError
import logging

def get_bucket_contents( bucket ):
    """Retorna una lista con los archivos contenidos en el bucket

    Params
        bucket: Objeto Bucket.
    Return
        list_files: Lista de archivos contenidos en el bucket.
    """
    list_files = []
    for file in bucket.objects.all():
        list_files.append(file.key)
    return list_files

def download_bucket_files( s3, bucket_name, file_names, dir ):
    """Descarga los archivos contenidos en el bucket s3.

    Params
        s3: Objeto s3.
        bucket_name: Nombre del bucket.
        file_names: Nombres de los archivos a descargar.
        dir: Carpeta donde queremos almacenar los archivos descargados.
    Return
        None
    """
    try:
        for file in file_names:
            s3.meta.client.download_file(Bucket=bucket_name, 
                                         Key=file, 
                                         Filename= f'{dir}\{file}')
        logging.info('Archivos {} descargados correctamente.'.format(file_names))
    except FileNotFoundError as e:
        print('No existe el directorio especificado')
    except ClientError as e:
        print('El/los archivos especificados a descargar no se encuentran en el bucket')

