import random
import uuid
from faker import Faker

# from utilities import department

fake = Faker()

def generate_employee(department_id):
    first_name = fake.first_name()
    last_name = fake.last_name()
    return {
        'employee_id': str(uuid.uuid4()),
        'first_name': first_name,
        'last_name': last_name,
        'department_id': department_id,
        'job_title': fake.job(),
        'salary': round(random.uniform(30000, 200000), 2),
        'hire_date': fake.date_this_decade(),
        'location': fake.city(),
        'age': random.randint(22, 65),
        'performance_score': random.randint(1, 5)
    }

print(generate_employee(1000))