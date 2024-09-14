import json
import time
import random
from faker import Faker
from confluent_kafka import Producer


# from utilities import department

fake = Faker()

department = {
    5000: 'Finance',
    6000: 'Marketing',
    7000: 'HR',
    8000: 'Engineering',
    9000: 'Sales'
}

def generate_employee(department_id):
    first_name = fake.first_name()
    last_name = fake.last_name()
    return {
        'employee_id': random.randint(100000, 999999),  # 6-digit number
        'first_name': first_name,
        'last_name': last_name,
        'department_name': department[department_id],
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
topic = 'sparkStreaming'

for i in range(1, 50):
    data = generate_employee(random.choice([5000, 6000, 7000, 8000, 9000]))

    # convert message to utf-8 encoding
    message = json.dumps(data).encode('utf-8')

    # Trigger the message delivery
    producer.produce(topic, message, callback=delivery_report)

    # Wait up to 1 second for events. Callbacks will be invoked during this method call if they are ready.
    producer.flush(1)

    time.sleep(5)
