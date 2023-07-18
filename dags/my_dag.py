from datetime import datetime
import requests
import csv
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import XCom

# Function to download the RSS feed and save it with the timestamp in the filename
def download_rss_feed(**context):
    rss_feed_url = 'https://timesofindia.indiatimes.com/rssfeedstopstories.cms'
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    filename = f'raw_rss_feed_{timestamp}.xml'
    response = requests.get(rss_feed_url)
    with open(filename, 'wb') as file:
        file.write(response.content)
    context['ti'].xcom_push(key='rss_filename', value=filename)

# Function to parse the downloaded XML file and extract desired information
def parse_rss_feed(**context):
    ti = context['ti']
    rss_filename = ti.xcom_pull(key='rss_filename')
    curated_filename = f'curated_{rss_filename.replace(".xml", ".csv")}'
    
    # Perform XML parsing and extract desired information
    # Save extracted information in a CSV file
    with open(curated_filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
    ti.xcom_push(key='curated_filename', value=curated_filename)

# Function to load the curated CSV file into a database
def load_to_database(**context):
    ti = context['ti']
    curated_filename = ti.xcom_pull(key='curated_filename')
 
# Create the DAG
dag = DAG(
    'rss_feed_pipeline',
    description='Download RSS feed, extract information, and load into a database',
    schedule_interval='0 23 * * *',  # Run every day at 11 PM
    start_date=datetime(2023, 7, 19),
    catchup=False
)

# Define the tasks
download_task = PythonOperator(
    task_id='download_rss_feed',
    python_callable=download_rss_feed,
    provide_context=True,
    dag=dag
)

parse_task = PythonOperator(
    task_id='parse_rss_feed',
    python_callable=parse_rss_feed,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_to_database',
    python_callable=load_to_database,
    provide_context=True,
    dag=dag
)

# Set task dependencies
download_task >> parse_task >> load_task
