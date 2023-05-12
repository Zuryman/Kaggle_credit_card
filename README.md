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


