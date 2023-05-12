# Kaggle_credit_card
This is a repo related to a Kaggle Credit Card Dataset Analysis
# ------------------------------------------------------------------------------------
Pipeline creado con Apache Spark (PySpark) y Apache Airflow en ambiente de Amazon (AWS)
Este REPO muestra el desarrollo de un Pipeline escalable en AWS, utilizando tecnicas de procesos paralelos en Apache spark
El analysis consiste en un analisis simple utilizando Pandas y Pylot.

El código basado en python analizar un núcleo compuesto de crédito de datos, este conjunto de datos se descarga de kaggle

## Requisitos previos

1. [Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
2. [Cuenta AWS](https://aws.amazon.com/de/) para ejecutar el pipeline en el entorno de la nube
3. Base de datos - En este proyecto, [PostgreSQL](https://aws.amazon.com/de/rds/postgresql/what-is-postgresql/) 
&emsp;
## 0. Configuración del entorno de nube en AWS

AWS proporciona [Amazon Managed Workflows for Apache Airflow (MWAA)](https://aws.amazon.com/de/blogs/aws/introducing-amazon-managed-workflows-for-apache-airflow-mwaa/) que hace que sea muy fácil ejecutar Apache Airflow en AWS.

1. Vaya a la [consola MWAA](https://console.aws.amazon.com/mwaa/home)  y "crear un nuevo entorno", despues seguir con la guia de  configuración paso a paso
2. Selecciona un [S3 bucket](https://s3.console.aws.amazon.com/) existente o crear uno nuevo, define la ruta desde la que se cargará el DAG de Airflow. El nombre del bucket debe empezar por "airflow".
3. Sube "requirements.txt" que contiene librerías de python para ejecutar el DAG. AWS las instalará a través de "pip install". 
4. Para simplificar,  creamos una "red pública" que nos permite iniciar sesión a través de Internet. Despues, dejamos que MWAA cree un nuevo "grupo de seguridad".
5. Para la clase de entorno, seleccionamos 'pw1.small' ya que se corresponde mejor con nuestra carga de trabajo DAG
6. Activamos 'Airflow task logs' utilizando la configuración por defecto. Esto permite tener información de registro que es especialmente útil para la depuración.
7. Crear un 'nuevo rol' o usar uno existente y complete la configuración haciendo clic en 'crear nuevo entorno'.
&emsp;
## 1a. Variables y conexiones para el entorno MWAA
MWAA proporciona variables para almacenar y recuperar contenido y configuraciones como  (clave-valor) dentro de Airflow. (JSON file)
```JSON
{
    "credit_card_analysis": {
        "bucket_name": "credit_card_analysis",
        "key1": "input/german_credit_data.csv", 
        "output_key": "output/results.parquet",
        "db_name": "postgres",
        "consumer_key": "{{KAGGLE KEY}}",
        "consumer_secret": "{{KAGGLE SECRET}}",
        "access_token": "{{KAGGLE ACCESS TOKEN}}",
        "access_token_secret": "{{{KAGGLE ACCESS TOKEN}}"
    }
}

```
En el repositorio se proporciona un [archivo de variables] de ejemplo : "airflow_variables.json" que contiene todas las variables utilizadas en este proyecto.
Airflow también permite definir objetos de conexión. En este vaso, necesitamos una conexión con 'AWS' (Airflow actúa como un sistema externo a AWS) y con la 'base de datos' en la que se almacenarán los resultados finales.
&emsp;
## 1b. Configuración general en el DAG de Airflow
Se define la información básica de configuración, como 'schedule_interval' o 'start_date' en la sección 'default_args' y dag del DAG. 
```Python
default_args = {
    'start_date': datetime(2021, 3, 8),
    'owner': 'Airflow',
    'filestore_base': '/tmp/airflowtemp/',
    'email_on_failure': True,
    'email_on_retry': False,
    'aws_conn_id': 'aws_zmora',
    'bucket_name': Variable.get('credit_card_analysis', deserialize_json=True)['bucket_name'],
    'postgres_conn_id': 'zmora',
    'output_key': Variable.get('credit_card_analysis',deserialize_json=True)['output_key'],
    'db_name': Variable.get('credit_card_analysis', deserialize_json=True)['db_name']
}
dag = DAG('credit_card_analysis',
          description='Extraer un dataset desde kaggle,  simular el kernell via Spark, salvar resultados a PostgreSQ y S3',
          schedule_interval='@daily',
          catchup=False,
          default_args=default_args,
          max_active_runs=1)
```
&emsp;
## 2. Tasks en el Airflow DAG

**Arquitectura básica  

 Las tareas de ML se ejecutan a través de [Amazon SageMaker](https://aws.amazon.com/de/sagemaker/), mientras que los análisis de datos complejos pueden realizarse de forma distribuida en [Amazon EMR](https://aws.amazon.com/de/emr/). En este REPO, se ejecuta el análisis de datos en un clúster de Amazon EMR utilizando [Apache Spark](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark.html) (a través de Python API PySpark).
Podemos escribir funciones personalizadas (por ejemplo, solicitar datos) o podemos hacer uso de módulos predefinidos que suelen estar ahí para desencadenar actividades externas (por ejemplo, análisis de datos en Spark en Amazon EMR).
Ejemplo de una función personalizada que luego se asigna a un 'PythonOperator' para funcionar como una tarea:
```Python
# custom function
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

# Task
create_schema = PythonOperator(
    task_id='create_schema',
    provide_context=True,
    python_callable=create_schema,
    op_kwargs=default_args,
    dag=dag,
)
```
&emsp;
## 3. Ejecutar Spark en Amazon EMR
**Permisos  
Cambia IAM_policy_configuration.json) a la siguiente configuración para permitir que MWAA interactúe con Amazon EMR. 
```JSON
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "elasticmapreduce:DescribeStep",
                "elasticmapreduce:AddJobFlowSteps",
                "elasticmapreduce:RunJobFlow"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": "iam:PassRole",
            "Resource": [
                "arn:aws:iam::{{AWS ID}}:role/EMR_DefaultRole",
                "arn:aws:iam::{{AWS ID}}:role/EMR_EC2_DefaultRole"
            ]
        }
    ]
}
```
&emsp;
**Motivación para utilizar Spark para el análisis de datos**

La API de Spark, es fácil de usar para los desarrolladores, reduce gran parte del trabajo pesado de la computación distribuida y se puede acceder a ella en varios lenguajes. En este caso [PySpark](https://pypi.org/project/pyspark/), que es una API de Python para interactuar con Spark a alto nivel. Esto significa que es adecuado para interactuar con un clúster existente, pero no contiene herramientas para configurar un nuevo clúster independiente.  
La lógica de paralelización de una arquitectura distribuida es el principal motor para acelerar el procesamiento y, por tanto, permitir la escalabilidad. El uso de DataFrame o Resilient Distributed Dataset (RDD) de Spark permite distribuir el cálculo de datos en un clúster.

Se utilizo  la plataforma de big data de Amazon [EMR](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-what-is-emr.html) para ejecutar este clúster Spark. Un clúster Spark puede caracterizarse por un nodo maestro que actúa como coordinador central y nodos trabajadores en los que se ejecutan las tareas/trabajos (=paralelización). Requiere una capa de almacenamiento distribuido que este caso es [HDFS](https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html) (Hadoop Distributed File System). El almacenamiento de objetos S3 se utiliza como nuestro almacenamiento de datos principal y HDFS como memoria temporal intermedia en la que el script accederá a los datos del deataset y escribirá los resultados. Temporal significa que los datos procesados en HDFS. La razón para utilizar HDFS es que es más rápido que escribir los resultados directamente en el cubo de S3.  

**Interacción entre Airflow y Amazon EMR**

Cada paso que se dará en el clúste, sera ejecutado por el DAG Airflow:
1.- Crear el clúster Spark proporcionando detalles de configuración específicos
2.- Ejecutar Hadoop para el almacenamiento de datos distribuidos simultáneamente.
3.- En cuanto a la configuración del clúster, utilizamos un nodo maestro y dos nodos de trabajo que se ejecutan en una [instancia] m5.xlarge (https://aws.amazon.com/de/ec2/instance-types/) (32 GB de RAM, 4 núcleos de CPU) dado el tamaño relativamente pequeño del conjunto de datos.
4.- Se activa y ejectuar Bootstrap para instalar bibliotecas python no estándar
```Python
     sudo pip3 install pandas-profiling
     sudo pip3 install psycopg2-binary
     sudo pip3 install plotly
     sudo pip3 install numpy
     sudo pip3 sklearn
     sudo pip3 xhboost
```
