import json
import os
import time

from faker import Faker
from faker.providers import internet
from fastapi import FastAPI, BackgroundTasks
from confluent_kafka import Producer
import logging

logger = logging.getLogger(__name__)

seconds_in_year = 3600 * 24 * 60 * 60
conf = {'bootstrap.servers': os.getenv("KAFKA_BROKERS", "")}
producer = Producer(**conf)
kafka_topic = os.getenv("KAFKA_TOPIC")
fake = Faker()
fake.add_provider(internet)

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")

def generate_single(occurrences: int):
    now = int(time.time())
    for i in range(occurrences):
        customer_id=f"{fake.uuid4(lambda uuid: str(uuid).replace('-', '')[:10])}{fake.uuid4(lambda uuid: str(uuid).replace('-', '')[-6:])}"
        msg = json.dumps({
            "Index": i,
            "Customer_Id": customer_id,
            "First_Name": fake.first_name(),
            "Last_Name": fake.last_name(),
            "Company": fake.company(),
            "City": fake.city(),
            "Country": fake.country(),
            "Phone_1": fake.phone_number(),
            "Phone_2": fake.phone_number(),
            "Email": fake.email(),
            "Subscription_Date": now - fake.random_int(1, seconds_in_year),
            "Website": fake.url(),
        })
        producer.produce(kafka_topic, value=msg.rstrip(), delivery_report=delivery_report)
    producer.flush()
    logger.info(f"Sent to kafka {occurrences} events")

app = FastAPI()

@app.get("/{occurrences}")
async def read_item(occurrences: int, background_tasks: BackgroundTasks):
    background_tasks.add_task(generate_single, occurrences)
    return {"message": "Generating data started"}
