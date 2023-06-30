from io import StringIO
from google.cloud.storage import Client
import sqlalchemy as db
from sqlalchemy import text
import pandas as pd
import os

class Extract():
    def __init__(self) -> None:
        self.process = 'Extractprocess'
        
    def read_mysql(self,table_name,db_name):        
        engine = db.create_engine(f"mysql://root:root@192.168.1.61:3310/{db_name}")
        conn = engine.connect()
        df = pd.read_sql_query(text(f'SELECT * FROM {table_name}'), con=conn)
        return df
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/user/app/ProyectoEndToEndPython/Proyecto/credenciales/dep12-386900-aa972d12d985.json"
    def read_cloud_storage(self, bucketName, fileName):
        client = Client()
        bucket = client.get_bucket(bucketName)
        blob = bucket.get_blob(fileName)
        downloaded_file = blob.download_as_text(encoding="utf-8")
        df = pd.read_csv(StringIO(downloaded_file))

        return df
