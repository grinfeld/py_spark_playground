import json
import os

from faker import Faker
from faker.providers import internet
from fastapi import FastAPI, BackgroundTasks
from confluent_kafka import Producer

def generate_single(occurrences: int, fake: Faker):

    for i in range(occurrences):
        customer_id=f"{fake.uuid4(lambda uuid: str(uuid).replace('-', '')[:10])}{fake.uuid4(lambda uuid: str(uuid).replace('-', '')[-6:])}"
        msg = json.dumps({
            "Index": i,
            "Customer Id": customer_id,
            "First Name": fake.first_name(),
            "Last Name": fake.last_name(),
            "Company": fake.company(),
            "City": fake.city(),
            "Country": fake.country(),
            "Phone 1": fake.phone_number(),
            "Phone 2": fake.phone_number(),
            "Email": fake.email(),
            "Subscription Date": fake.date("%Y-%m-%d"),
            "Website": fake.url(),
        })
        producer.produce(kafka_topic, value=msg.rstrip(), key=customer_id)

app = FastAPI()
conf = {'bootstrap.servers': os.getenv("KAFKA_BROKERS", "")}
producer = Producer(**conf)
kafka_topic = os.getenv("KAFKA_TOPIC")
faker = Faker()
faker.add_provider(internet)

@app.get("/{occurrences}")
async def read_item(occurrences: int, background_tasks: BackgroundTasks):
    background_tasks.add_task(generate_single, occurrences, faker)
    return {"message": "Generating data started"}
