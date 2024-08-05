import json
import time
import random
from faker import Faker
from confluent_kafka import Producer


# from utilities import department

fake = Faker()

department = {
    "Finance": 5000,
    "Marketing": 6000,
    "HR": 7000,
    "Engineering":  8000,
    "Sales": 9000
    }

def generate_employee(department_id):
    first_name = fake.first_name()
    last_name = fake.last_name()
    return {
        'employee_id': random.randint(100000, 999999),  # 6-digit number
        'first_name': first_name,
        'last_name': last_name,
        'department_id': department_id,
        'job_title': fake.job(),
        'salary': round(random.uniform(30000, 200000), 2),
        'hire_date': fake.date_between(start_date='-10y', end_date='today').isoformat(),
        'location': fake.city(),
        'age': random.randint(22, 55),
        'performance_score': random.randint(1, 5)
    }

# Delivery callback to confirm message delivery
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


# Configuration settings for Kafka Producer
kafka_conf = {
    'bootstrap.servers':'localhost:9092'
    }

# Create Producer instance
producer = Producer(kafka_conf)

# Producing employee data to Kafka topic
topic = 'spark-streaming'

for i in range(1, 100):
    data = generate_employee(department["HR"])

    # convert message to utf-8 encoding
    message = json.dumps(data).encode('utf-8')

    # Trigger the message delivery
    producer.produce(topic, message, callback=delivery_report)

    # Wait up to 1 second for events. Callbacks will be invoked during this method call if they are ready.
    producer.flush(1)

    time.sleep(5)
