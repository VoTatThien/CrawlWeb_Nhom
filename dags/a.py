from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from pymongo import MongoClient
from kafka import KafkaProducer
from bson import ObjectId
import json

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 7, 7),  # Ngày bắt đầu hợp lệ
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
dag = DAG(
    dag_id='mongodb_to_kafka',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False
)

run_app_crawler_compose = BashOperator(

    task_id="run_app_crawler_compose",
    bash_command="docker-compose up -d app_crawler",
    dag=dag
)

def json_serializer(document):
    return json.dumps(document, default=lambda x: str(x) if isinstance(x, ObjectId) else x).encode('utf-8')

def send_mongo_data_to_kafka():
    try:
        # Connect to MongoDB
        client = MongoClient('mongodb://mongodb:27017')
        db = client['db_goodread']
        collection = db['tb_book']

        # Connect to Kafka
        producer = KafkaProducer(
            bootstrap_servers='kafka:29092',
            value_serializer=json_serializer
        )

        # Read from MongoDB and send to Kafka
        for document in collection.find():
            producer.send('book', document)
        
        producer.flush()  # Ensure all records are sent
    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        producer.close()  # Close producer connection

# Define the task in Airflow
send_data_task = PythonOperator(
    task_id='send_mongo_data_to_kafka_task',
    python_callable=send_mongo_data_to_kafka,
    dag=dag
)

# Task execution order
run_app_crawler_compose >> send_data_task