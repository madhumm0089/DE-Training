import json, random
import time as t
from datetime import date, timedelta, time

from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='madhu-kafka-namespace.servicebus.windows.net:9093',
    security_protocol='SASL_SSL',
    sasl_mechanism='PLAIN',
    sasl_plain_username='$ConnectionString',
    sasl_plain_password='Endpoint=sb://madhu-kafka-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=zdOcO2ULbck4a4Ek+cYKqctucIDvwXdsB+AEhC292q0=',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)


users = [f"U{uid}" for uid in range(1, 10000)]

locations = ["Delhi","Mumbai","Bangalore","Chennai","Kolkata","Hyderabad","Jaipur","Goa",
             "Varanasi","Leh","Darjeeling","Udaipur","Mysore","Shillong","Kochi", "Mandya"]

card_types = ["Visa", "MasterCard", "RuPay", "American Express", "Diners Club"]

merchants = [
    "Amazon", "Flipkart", "Walmart", "Target", "Best Buy",
    "Starbucks", "McDonald's", "Shell", "ExxonMobil", "Apple Store"
]

status = ["Success","Success","Success","Success","Success","Success", "Failed"]
is_fraud = ["yes", "no","no","no","no",]

def random_date():
    start_date = date(2018,1,1)
    end_date = date(2025, 7, 1)
    days_between = (end_date - start_date).days
    random_days = random.randint(0, days_between)
    return start_date + timedelta(days=random_days)

def random_time():
    hour = random.randint(0, 23)     
    minute = random.randint(0, 59)    
    second = random.randint(0, 59)    
    return time(hour, minute, second)

def card_number_generate():
    card_number = ''.join(str(random.randint(0, 9)) for _ in range(16))
    return card_number


def generate_txn():
    location = random.choice(locations)
    return {
        "transactionId": f"TX{random.randint(1000000, 9999999)}",
        "userId": random.choice(users),
        "cardNumber": card_number_generate(),
        "amount": round(random.uniform(10, 2000), 2),
        "location":random.choice(locations),
        "date": random_date().isoformat(),
        "time":random_time().strftime("%H:%M:%S"),
        "cardType": random.choice(card_types),
        "merchant": random.choice(merchants),
        "payment_status":random.choice(status), 
        "is_fraud":random.choice(is_fraud)
    }

while True:
    txn = generate_txn()
    print("Sending:", txn)
    producer.send("transactions", key=txn["transactionId"], value=txn)
    t.sleep(0.01)