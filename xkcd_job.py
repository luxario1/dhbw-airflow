from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.http_download_operations import HttpDownloadOperator
from airflow.operators.zip_file_operations import UnzipFileOperator
from airflow.operators.hdfs_operations import HdfsPutFileOperator, HdfsGetFileOperator, HdfsMkdirFileOperator
from airflow.operators.filesystem_operations import CreateDirectoryOperator
from airflow.operators.filesystem_operations import ClearDirectoryOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import asyncio
import requests
import json
import os
from pathlib import Path
from multiprocessing import Pool

args = {
    'owner': 'airflow'
}

#Python-Methode, welche von download_xkcd() benutzt wird
def parallel_trigger(i):
    if i != 404 and i != 1037 and i != 1331: #Fehlerhafte Daten aus der API
        apiObj = requests.get(f'https://xkcd.com/{i}/info.0.json') #holen der Infos aus der API
        jsonObj = apiObj.json() #Ergebnis in JSON parsen

        if (os.path.exists(f'/home/airflow/xkcd/raw/{jsonObj["year"]}')): #wenn der Pfad lokal schon existiert...
            Path(f'/home/airflow/xkcd/raw/{jsonObj["year"]}/{jsonObj["num"]}.json').write_text(json.dumps(jsonObj)) #...erstelle eine neue JSON-Datei fuer dieses Comic in diesem Pfad
        else: #wenn der Pfad noch nicht existiert...
            os.makedirs(f'/home/airflow/xkcd/raw/{jsonObj["year"]}') #...erstelle den Pfad fuer das Jahr des Comics...
            Path(f'/home/airflow/xkcd/raw/{jsonObj["year"]}/{jsonObj["num"]}.json').write_text(json.dumps(jsonObj)) #...und erstelle dazu eine neue Datei

#Python-Methode, die vom PythonOperator unten genutzt wird
def download_xkcd():

    lenReq = requests.get('https://xkcd.com/info.0.json') #hole den neuesten Comic...
    lenObj = lenReq.json() #...parse zu JSON...

    maxNum = range(1, int(lenObj["num"])+1) #...erzeuge eine Range von Nummern fuer die existierenden Comics...
    #Fuehrt die Funktion parrallel_trigger asynchron parallel fuer jede Zahl im Array aus
    pool = Pool()
    pool.map(parallel_trigger, maxNum)

#Definition des eigentlichen DAG's
dag = DAG('xkcdJob_main', default_args=args, description='xkcd comics',
          schedule_interval='56 18 * * *',
          start_date=datetime(2019, 10, 16), catchup=False, max_active_runs=1)

#Definition des Bashoperator, welcher lokal /home/airflow/xkcd erzeugt
create_local_dir_xkcd = CreateDirectoryOperator(
    task_id='create_local_dir_xkcd',
    path='/home/airflow',
    directory='xkcd',
    dag=dag,
)

#Definition des Bashoperator, welcher lokal /home/airflow/xkcd/raw erzeugt
create_local_dir_raw = CreateDirectoryOperator(
    task_id='create_local_dir_raw',
    path='/home/airflow/xkcd',
    directory='raw',
    dag=dag,
)

#Definition des Bashoperator, welcher Platzhalter lokal erstellt
create_placeholder = BashOperator(
    task_id='create_placeholder',
    bash_command='touch /home/airflow/xkcd/raw/placeholder.py',
    dag=dag,
)

#Definition des Bashoperator, welcher Platzhalter ins HDFS kopiert
copy_placeholder_to_hdfs = BashOperator(
    task_id='copy_placeholder_to_hdfs',
    bash_command='/home/airflow/hadoop/bin/hadoop fs -put /home/airflow/xkcd/raw/placeholder.py /user/hadoop/xkcd/raw',
    dag=dag,
)

#Definition des Bashoperator, welcher lokale Daten loescht
clear_xkcddata_local = BashOperator(
    task_id='clear_xkcddata_local',
    bash_command='rm -r /home/airflow/xkcd/raw/*',
    dag=dag,
)

#Definition des Bashoperator, welcher /user/hadoop/xkcd in HDFS erzeugt
create_path_hdfs_xkcd = BashOperator(
    task_id='create_path_hdfs_xkcd',
    bash_command='/home/airflow/hadoop/bin/hadoop fs -mkdir -p /user/hadoop/xkcd',
    dag=dag,
)

#Definition des Bashoperator, welcher /user/hadoop/xkcd/raw in HDFS erzeugt
create_path_hdfs_raw = BashOperator(
    task_id='create_path_hdfs_raw',
    bash_command='/home/airflow/hadoop/bin/hadoop fs -mkdir -p /user/hadoop/xkcd/raw',
    dag=dag,
)

#Definition des Bashoperator, welcher Inhalte in HDFS loescht
clear_xkcddata_hdfs = BashOperator(
    task_id='clear_xkcddata_hdfs',
    bash_command='/home/airflow/hadoop/bin/hadoop dfs -rm -r /user/hadoop/xkcd',
    dag=dag,
)

#Definition des Bashoperator, welcher Python Methode aufruft, welche xkcd-Daten downloadet
download_xkcd = PythonOperator(
    task_id='download_xkcd',
    python_callable=download_xkcd,
    dag=dag,
)

#Definiton des Bashoperator, welcher lokale Daten auf das HDFS pusht
push_xkcddata_hdfs = BashOperator(
    task_id='push_xkcddata_hdfs',
    bash_command='/home/airflow/hadoop/bin/hadoop fs -put /home/airflow/xkcd /user/hadoop',
    dag=dag,
)

#Definition PySpark-Operator fuer das Prozessieren der Daten
pyspark_raw_to_final = SparkSubmitOperator(
    task_id='pyspark_raw_to_final',
    conn_id='spark',
    application='/home/airflow/airflow/python/raw_to_final.py',
    total_executor_cores='2',
    executor_cores='2',
    executor_memory='2g',
    num_executors='2',
    name='spark_raw_to_final',
    verbose=True,
    dag = dag
)

#Reihenfolge Workflow
create_local_dir_xkcd >> create_local_dir_raw >> create_placeholder >> copy_placeholder_to_hdfs
copy_placeholder_to_hdfs >> clear_xkcddata_hdfs
copy_placeholder_to_hdfs >> clear_xkcddata_local >> download_xkcd
download_xkcd >> push_xkcddata_hdfs >> pyspark_raw_to_final
clear_xkcddata_hdfs >> push_xkcddata_hdfs
create_path_hdfs_xkcd >> create_path_hdfs_raw >> copy_placeholder_to_hdfs
