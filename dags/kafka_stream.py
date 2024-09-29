from datetime import datetime
import time
from kafka import KafkaProducer
import json
import uuid
import requests
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator

logging.basicConfig(level=logging.INFO)

 
def get_data():
    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    logging.info("Data fetched from API")
    return res['results'][0]

def format_data(res):
    location = res['location']
    data = {
        'id': str(uuid.uuid4()),
        'first_name': res['name']['first'],
        'last_name': res['name']['last'],
        'gender': res['gender'],
        'address': f"{location['street']['number']} {location['street']['name']}, {location['city']}, {location['state']}, {location['country']}",
        'post_code': location['postcode'],
        'email': res['email'],
        'username': res['login']['username'],
        'dob': res['dob']['date'],
        'registered_date': res['registered']['date'],
        'phone': res['phone'],
        'picture': res['picture']['medium']
    }
    logging.info("Data formatted")
    return data

def send_data_to_kafka():
    
    topic = "user_created"  # Fixed topic name
    
    producer = KafkaProducer(
        # bootstrap_servers='localhost:9092',
        bootstrap_servers=['broker:29092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    

    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60: #1 minute
            break
        try:
            data = format_data(get_data())

            logging.info("Sending data to Kafka...")
            future = producer.send(topic, value=data)
            producer.flush()  # Ensure all messages are sent

            record_metadata = future.get(timeout=10)
            
            logging.info(f"Data sent to Kafka topic '{topic}': {data}")
            logging.info(f"Topic: {record_metadata.topic}")
            logging.info(f"Partition: {record_metadata.partition}")
            logging.info(f"Offset: {record_metadata.offset}")
            
        except Exception as e:
            logging.error(f"Error sending data to Kafka: {e}")
            # producer.close()
            continue


default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2024, 9, 3),  
}

 
with DAG(
    'user_automation',
    default_args=default_args,
    schedule='@daily',
    catchup=False  # Do not run for past dates if the DAG has not been run before
) as dag:

    # Define the task that streams data from an API and sends it to Kafka
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=send_data_to_kafka,
        dag=dag
    )

# send_data_to_kafka()