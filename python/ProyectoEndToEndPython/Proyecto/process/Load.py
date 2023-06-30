from io import StringIO
import io
import pandas as pd
import os
from google.cloud.storage import Client
import sqlalchemy as db


class load():
    def __init__(self) -> None:
        self.process = 'Load Process'
        
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/user/app/ProyectoEndToEndPython/Proyecto/credenciales/dep12-386900-aa972d12d985.json"
    def load_to_cloud_storage(self, df, bucketName, fileName):
        client = Client()
        bucket = client.get_bucket(bucketName)
        bucket.blob(fileName).upload_from_string(df.to_csv(index=False), 'text/csv')

    def load_to_mysql(self, df, table_name, db_name):
        engine = db.create_engine(f"mysql://root:root@192.168.1.61:3310/{db_name}")
        conn = engine.connect()
        df.to_sql(table_name, con=conn, if_exists='replace', index=False)
        conn.close()