# Modulos de Airflow
from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.hooks.base_hook import BaseHook

# otros modulos
import boto3, json, pprint, requests, textwrap, time, logging
import os
from datetime import datetime
from pandas_profiling import ProfileReport
from typing import Optional, Union
from typing import Iterable
from datetime import datetime
from datetime import timedelta
import logging
import pandas as pd
import numpy as np
import re
import io
import shlex
import tweepy
import pytz

#****************************************************************************************
# 4.- Modulos para exploración
#****************************************************************************************
# it's a library that we work with plotly
import plotly.offline as py 
py.init_notebook_mode(connected=True) # this code, allow us to work with offline plotly version
import plotly.graph_objs as go # it's like "plt" of matplot
import plotly.tools as tls # It's useful to we get some tools of plotly
import warnings # This library will be used to ignore some warnings
from collections import Counter # To do counter of some features


log = logging.getLogger(__name__)
TZ=pytz.timezone('America/Mazatlan')
TODAY=datetime.now(TZ).strftime('%Y-%m-%d')
URL='https://www.kaggle.com/datasets/uciml/german-credit?select=german_credit_data.csv'
PATH='/opt/airlfow/00_input_data/credit_card_data.csv'
OUTPUT_DQ='/opt/airflow/00_input_data/data_quality_report_{}.html'.format(TODAY)



# =============================================================================

# 1.1.- Setting Amazon EMR (Spark, Hadoop)
#------------------------------------------------------------------------------
# Airflow offers pre-defined modules to quickly interact with Amazon EMR.
# The code below shows Amazon EMR cluster with Spark (PySpark)
# and Hadoop application is created using `EmrCreateJobFlowOperator()
# =============================================================================



JOB_FLOW_OVERRIDES = {
    "Name": "credit_card_analysis",
    "ReleaseLabel": "emr-5.33.0",
    "LogUri": "s3n://credit_card_analysis/logs/",   # Primero tenemos que subir el archivo .sh al path en el bucket, primero hay que crear las carpetas
    "BootstrapActions": [                           # Con Bootstrap se ejecutara el script y se instalaran las lib necesarias
        {'Name': 'install python libraries',        # En este caso son las librerias de spark, pandas etc..
                'ScriptBootstrapAction': {         
                'Path': 's3://credit_card_analysis/scripts/python-libraries.sh'}
                            }
                        ],
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}], # El cluster EMR debe tener HDFS y Spark
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {
                    "PYSPARK_PYTHON": "/usr/bin/python3",
                    "spark.pyspark.virtualenv.enabled": "true",
                    "spark.pyspark.virtualenv.type":"native",
                    "spark.pyspark.virtualenv.bin.path":"/usr/bin/virtualenv"
                    },
                }
            ],
        }
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core - 2",
                "Market": "SPOT", # SPOT  son instancias de  uso "según disponibilidad"
                "InstanceRole": "CORE", # se crea un nodo master y 2 nodos exclavos, como el archivo es muy pequeño
                "InstanceType": "m5.xlarge", #Esto es sobrado, pero aqui se pueden definir N cantidad de nodos de proces
                "InstanceCount": 2,          # Si los archivos a procesar son mas grandes, si no se requieren mas nodos
            },                               # Las instancias no se activan
        ],
        "Ec2SubnetId": "subnet-0427e49b255238212",
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False, # Esto nos permite terminar la programacion del cluster
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}

# definir los spark jobs que se ejecutan en EMR create_emr_cluster

SPARK_STEPS = [ # Aqui voy a mover el archivo  parquet, se mueve a Hadoop para procesarlo ya que es mas rapido
    {
        "Name": "mover raw_data S3 a HDFS",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=s3://credit_card_analysis/output/results.parquet",
                "--dest=/results",
            ],
        },
    },
    {
        "Name": "run curation_step", # En este paso se procesa el archivo con spark, el proceso del kernell
        "ActionOnFailure": "CANCEL_AND_WAIT", # que se proporciono, es mas orientado a analisis.
        "HadoopJarStep": {                    # Respecto a un curation, es minimo, igual hice un curation con python
            "Jar": "command-runner.jar",      # como pruebas solamente
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3://credit_card_analysis/scripts/curation_step.py",
            ],
        },
    },
    {
        "Name": "mover el resultado final de credit_analysis  desde HDFS a S3",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": { # En este Job regresamos los resultados del proceso al Bucket a la carpeta resultados
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=/output",
                "--dest=s3://credit_card_analysis/results/",
            ],
        },
    },

  # Estos pasos anteriores, son para que se procese el dataset es decir se ejecute curation_step.py
  # En los pasos siguientes, es la misma secuencia, se mueve el archivo ya curado, se mueve a Hadoop y se hace el
  # Entrenamiento con Spark 

   {
        "Name": "mover raw_data S3 a HDFS",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=s3://credit_card_analysis/output/results.parquet",
                "--dest=/results",
            ],
        },
    },
    {
        "Name": "run training_step", 
        "ActionOnFailure": "CANCEL_AND_WAIT", 
        "HadoopJarStep": {                    
            "Jar": "command-runner.jar",      
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3://credit_card_analysis/scripts/curation_step.py",
            ],
        },
    },
    {
        "Name": "mover el resultado final de credit_analysis  desde HDFS a S3",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": { 
            "Jar": "command-runner.jar",
            "Args": [
                "s3-dist-cp",
                "--src=/output",
                "--dest=s3://credit_card_analysis/results/",
            ],
        },
    },

]

# =============================================================================
# 1.2. Configuracion del DAG
# =============================================================================

default_args = {
    'start_date': datetime(2023, 3, 8),
    'owner': 'Airflow',
    'filestore_base': '/tmp/airflowtemp/',
    'email_on_failure': True,
    'email_on_retry': False,
    'aws_conn_id': 'aws_zmora',
    'emr_conn_id' : 'emr_zmora',
    'bucket_name': Variable.get('credit_card_analysis', deserialize_json=True)['bucket_name'],
    'postgres_conn_id': 'zmora',
    'output_key': Variable.get('kaggle',deserialize_json=True)['output_key'],
    'db_name': Variable.get('credit_card_db', deserialize_json=True)['db_name']
}

dag = DAG('Credit_analysis',
          description='Extraer un dataset desde kaggle,  simular el kernell via Spark, salvar resultados a PostgreSQ y S3',
          schedule_interval='@daily',
          catchup=False,
          default_args=default_args,
          max_active_runs=1,
          tags=['credit_card','kaggle credit analysis'])


# =============================================================================
# 2. Functions
# =============================================================================


#------------------------------------------------------------------------------
#  Esta funcion crea schemas y tablas directamente en la BD de PostgreSQL
#  Donde el dataset final sera cargado
#------------------------------------------------------------------------------
def create_schema(**kwargs):
    '''
    1. connectar a la Base de Datos en Postgres 
    2. crear schema y tabla donde se subiran los datos
    3. detonar el query
    '''

    pg_hook = PostgresHook(postgres_conn_id=kwargs['postgres_conn_id'], schema=kwargs['db_name'])
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    log.info('inicializando la conexion')
    sql_queries = """

    CREATE SCHEMA IF NOT EXISTS credit_card_schema;
    CREATE TABLE IF NOT EXISTS credit_card_schema.card_data(
        "Age" numeric,
        "Sex" varchar(8),
        "Job" numeric,
        "Housing" varchar(6),
        "Saving_account" varchar(20),
        "Check_account" nmeric,
        "Duration" numeric,
        "Purpose" varchar(40),
    );
     CREATE TABLE IF NOT EXISTS credit_card_schema.data_flow(
        "batch_nr" numeric,
        "timestamp" timestamp,
        "step_airflow" varchar(256),
        "source" varchar(256),
        "destination" varchar(256)
    );
    
    """

    # ejecutar query
    cursor.execute(sql_queries)
    conn.commit()
    log.info("schema y tabla creados")



def get_profile(**kwargs):

    #read the CSV
    df_credit=pd.read_csv(PATH,index_col=0)
    profile=ProfileReport(df_credit,title='Revision de la calidad de los datos')
    profile.to_file(OUTPUT_DQ)
    log.info('leer csv y obtener el perfilamiento de datos')

  
def saving_to_AWS(** kwargs):
     
     df_credit=pd.read_csv(PATH,index_col=0)

     # establecer conexion al bucket de S3
     bucket_name = kwargs['bucket_name']
     key = Variable.get('credit_card_get_csv', deserialize_json=True)['key1']
     s3 = S3Hook(kwargs['aws_conn_id'])
     log.info('conexion establecida al bucket de S3')

    # generar la instancia para la tarea
     task_instance = kwargs['ti']
     print(task_instance)
     log.info('Obtener la instancia de la tarea')

   
    # preparar el archivo para enviarlo a S3 en formato parquet
     parquet_buffer = io.BytesIO()
     data_parquet=df_credit.to_parquet(parquet_buffer)

    # salvando el pandas dataframe como archivo parquet
     s3 = s3.get_resource_type('s3')

    #  obtener el data type object, key y el object conec para el bucket
     data = parquet_buffer.getvalue()
     object = s3.Object(bucket_name, key)

    # Escribir el archivo en el bucket en el path definido en el key
     object.put(Body=data)
     log.info('Finished saving data to s3')

     # guardar flujo de datos (Actualizar historial)

     # conectar a la base de datos PostgreSQL
     pg_hook = PostgresHook(postgres_conn_id=kwargs['postgres_conn_id'], schema=kwargs['db_name'])
     conn = pg_hook.get_conn()
     cursor = conn.cursor()

     log.info('Conexion inicializada')
     log.info('Registrando los datos')

     s = """INSERT INTO credit_card_schema.data_flow(batch_nr,timestamp, step_airflow, source, destination) VALUES (%s, %s, %s, %s, %s, %s)"""

     # assign information
     batch_nr=datetime.now().strftime('%Y%m%d')
     timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
     step_airflow="get_data"
     source = "External: Kaggle data"
     destination = 's3://' + bucket_name + '/' + key

     # crear la  list a de  informacion para transmitir a la base de datos
     obj = []
     obj.append([batch_nr,
                timestamp,
                step_airflow,
                source,
                destination])

     # detonar query
     cursor.executemany(s, obj)
     conn.commit()

     log.info('Historial actualizado')
     return

  
# salvar datos a la Base de Datos Postgres
def save_result_to_postgres_db(**kwargs):

       # establecer conexion al Bucket S3
    bucket_name = kwargs['bucket_name']
    s3 = S3Hook(kwargs['aws_conn_id'])
    log.info("Established connection to S3 bucket")

    # Listar todos los keys que se encuentren en el subpath credit
    keys = s3.list_keys(bucket_name, prefix="credit/", delimiter="")

    # accessar al bucket
    s3 = s3.get_resource_type('s3')
    response = s3.Object(bucket_name, keys).get()
    bytes_object = response['Body'].read()

    # convertir a Dataframe, el  archivo parquet previamente subido a bucket S3
    df = pd.read_parquet(io.BytesIO(bytes_object))

    log.info('transformando los datos parquet a pandas dataframe')

    # connectar a la base de datos Postgre
    pg_hook = PostgresHook(postgres_conn_id=kwargs['postgres_conn_id'], schema=kwargs['db_name'])
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    log.info('Conexion establecida')
    log.info('Insertando fila por fila en la base de datos')

    # insertando filas en la tabla PostgresSQL
    s = """INSERT INTO credit_card_schema.card_data(Age,Sex, Job, Housing, Saving_Account, Check_account,Duration,Purpose) VALUES (%s, %s, %s, %s,%s,%s,%s,%s)"""
    
    # create list de registros que se guardaran en la Base de Datos
    for index in range(len(df)):
        obj = []

        obj.append([df.Age[index],
                    df.Sex[index],
                    df.Job[index],
                    df.Housing[index],
                    df.Saving_Account[index],
                    df.Check_account[index],
                    df.Duration[index],
                    df.Purpose[index]])

    # detonar query
        cursor.executemany(s, obj)
        conn.commit()

    log.info('Proceso finalizado, los datos se han salvado en la Base de Datos postgres')

      

# =============================================================================
# 3. Create Operators
# =============================================================================

download_csv=BashOperator(
        task_id='download_csv',
        bash_command='curl -o {{params.path}}{{params.url}}',
        params={
            'path':PATH,
            'url' :URL
        }
    )

create_schema = PythonOperator(
    task_id='create_schema',
    provide_context=True,
    python_callable=create_schema,
    op_kwargs=default_args,
    dag=dag,

)

get_profile = PythonOperator(
    task_id='get_twitter_data',
    provide_context=True,
    python_callable=get_profile,
    op_kwargs=default_args,
    dag=dag,

)

save_result_to_postgres_db = PythonOperator(
    task_id='save_result_to_postgres_db',
    provide_context=True,
    python_callable=save_result_to_postgres_db,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    op_kwargs=default_args,
    dag=dag,

)

saving_to_AWS = PythonOperator(
    task_id='saving_to_aws',
    provide_context=True,
    python_callable=saving_to_AWS,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    op_kwargs=default_args,
    dag=dag,

)

create_emr_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_zmora",
    emr_conn_id="emr_zmora",
    dag=dag,
)


step_adder = EmrAddStepsOperator(
    task_id="add_steps",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_zmora",
    steps=SPARK_STEPS,
    dag=dag,
)

last_step = len(SPARK_STEPS) - 1 # Este dato permitira que el sensor detecte el ultimo paso registrado


step_checker = EmrStepSensor(
    task_id="watch_step",
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')["
    + str(last_step)
    + "] }}",
    aws_conn_id="aws_zmora",
    dag=dag,
)

# terminate the EMR cluster
terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_zmora",
    dag=dag,
)

start_data_pipeline = DummyOperator(task_id="start_data_pipeline", dag=dag)
end_data_pipeline = DummyOperator(task_id = "end_data_pipeline", dag=dag)



# =============================================================================
# 4. Indicating the order of the dags
# =============================================================================


start_data_pipeline >> download_csv >> get_profile >> create_schema >> saving_to_AWS >> save_result_to_postgres_db >> create_emr_cluster >> step_adder
step_adder >> step_checker >> terminate_emr_cluster >> save_result_to_postgres_db
save_result_to_postgres_db >> end_data_pipeline
