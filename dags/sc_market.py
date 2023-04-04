import datetime
import pandas as pd, glob, os, airflow.utils.dates, sys

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.python import PythonSensor
from pathlib import Path

dag = DAG(
    dag_id="sc_market",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval= None,
    description="Alur kerja batch untuk get data promosi supermarket, mendemonstrasikan PythonSensor.",
    # schedule_interval='*/5 * * * *',
)

path    = "/home/airflow/data"
dateTime = datetime.datetime.now()     

def create_file():
    directory = Path(path)
    directory.mkdir(parents=True, exist_ok=True)

    data1 = {
            "sku": ["P01", "P02", "P03", "P04", "P05", "P06"],
            "name": ["Indomie Goreng", "Indomie Soto", "Saos Sambal", "Sabun Cair", "Bihun Goreng", "Gosok Gigi"],
            "stock": [5, 3, -3, 15, 2, 7],
    }

    data2 = {
            "sku": ["P01", "P02", "P03", "P04", "P05", "P06"],
            "name": ["Indomie Goreng", "Indomie Soto", "Saos Sambal", "Sabun Cair", "Bihun Goreng", "Gosok Gigi"],
            "stock": [3, 5, 4, 1, 8, 5],
    }

    df1 = pd.DataFrame(data1)
    df2 = pd.DataFrame(data2)
    
    df1.to_csv(str(path+'/produk_'+dateTime.strftime("%Y%m%d%H%M")+'_1.csv'), index=False)

    df2.to_csv(str(path+'/produk_'+dateTime.strftime("%Y%m%d%H%M")+'_2.csv'), index=False)

    print("Sukses Membuat File Produk. . .")

def processing_data():
    allFiles = glob.glob(str(path+"/*.csv"), recursive=True)

    for files in allFiles:
        #Postgres Koneksi
        conn = PostgresHook(postgres_conn_id='postgre_airflow').get_conn()
        cursor = conn.cursor()

        with open(files, 'r') as f:
            df = pd.read_csv(f)

            print("Process insert data from csv. . .")
            
            for index, row in df.iterrows():                
                cursor.execute('select count(*) from produk where sku = %s', (row['sku'],))
                result = cursor.fetchone()

                for res in result:
                    print(res)
                print("Total number of rows on sku "+row['sku']+" : ", res)
                
                # sys.exit()
                if res < 1:
                    params = (row['sku'], row['name'], row['stock'])

                    cursor.execute(
                        "INSERT INTO produk (sku, name, stock) VALUES (%s, %s, %s)", params
                    )
                else:
                    params = (row['stock'], row['sku'])

                    cursor.execute(
                        "UPDATE produk set stock = %s where sku = %s", params
                    )
            
            conn.commit()



def delete_file():
    allFiles = glob.glob(str(path+"/*.csv"), recursive=True)

    for files in allFiles:
        os.remove(files)
        print("Delete File. . .")


createTable = PostgresOperator(
    task_id = 'create_table',
    postgres_conn_id = 'postgre_airflow',
    sql = '''
         create table if not exists produk(
            id SERIAL PRIMARY KEY,
            sku VARCHAR NOT NULL,
            name VARCHAR NOT NULL,
            stock INTEGER NOT NULL not null default 0
        );
    ''',
    dag = dag
)

createFile = PythonOperator(
    task_id='create_file',
    python_callable = create_file,
    dag=dag
)

processing = PythonOperator(
    task_id='processing',
    python_callable = processing_data,
    dag=dag
)

deleteFile = PythonOperator(
    task_id='delete_file',
    python_callable = delete_file,
    dag=dag
)

finish = BashOperator(
    task_id = 'finish',
    bash_command = 'echo "Selesai"',
    dag = dag
)

createTable >> createFile >> processing >> deleteFile >> finish