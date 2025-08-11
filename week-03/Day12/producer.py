# import json, random, time
# from datetime import datetime
# from kafka import KafkaProducer

# producer = KafkaProducer(
#     bootstrap_servers='madhu-kafka-namespace.servicebus.windows.net:9093',
#     security_protocol='SASL_SSL',
#     sasl_mechanism='PLAIN',
#     sasl_plain_username='$ConnectionString',
#     sasl_plain_password='Endpoint=sb://madhu-kafka-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=zdOcO2ULbck4a4Ek+cYKqctucIDvwXdsB+AEhC292q0=',
#     value_serializer=lambda v: json.dumps(v).encode('utf-8'),
#     key_serializer=lambda k: k.encode('utf-8')
# )

# users = ['U100', 'U101', 'U102']
# locations = ['mumbai', 'bangalore', 'mandya', 'mysore', 'london']
# is_fraud= ['yes', 'no']

# def generate_txn():
#     return {
#         "transactionId": f"TX{random.randint(1000, 9999)}",
#         "cardNumber": f"9876-XXXX-XXXX-{random.randint(1000, 9999)}",
#         "amount": round(random.uniform(100, 100000), 2),
#         "location": random.choice(locations),
#         "timestamp": datetime.utcnow().isoformat(),
#         "userId": random.choice(users),
#         "is_fraud":random.choice(is_fraud)
#     }

# while True:
#     txn = generate_txn()
#     print("Sending:", txn)
#     producer.send("transactions", key=txn["transactionId"], value=txn)
#     time.sleep(1)


import json, random, time
from datetime import datetime
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

# Larger user list
users = [f"U{uid}" for uid in range(1000, 3000)]  # 2000 users

# More detailed locations (city, country)
locations = [
    {"city": "Mumbai", "country": "India"},
    {"city": "Bangalore", "country": "India"},
    {"city": "Mandya", "country": "India"},
    {"city": "Mysore", "country": "India"},
    {"city": "London", "country": "UK"},
    {"city": "New York", "country": "USA"},
    {"city": "Berlin", "country": "Germany"},
    {"city": "Tokyo", "country": "Japan"},
    {"city": "Sydney", "country": "Australia"},
]

card_types = ['Visa', 'MasterCard', 'Amex', 'Discover']

merchants = [
    "Amazon", "Flipkart", "Walmart", "Target", "Best Buy",
    "Starbucks", "McDonald's", "Shell", "ExxonMobil", "Apple Store"
]

is_fraud = ['yes', 'no']

def generate_txn():
    location = random.choice(locations)
    return {
        "transactionId": f"TX{random.randint(1000000, 9999999)}",
        "cardNumber": f"9876-XXXX-XXXX-{random.randint(1000, 9999)}",
        "amount": round(random.uniform(10, 2000), 2),
        "location": {
            "city": location["city"],
            "country": location["country"]
        },
        "timestamp": datetime.utcnow().isoformat(),
        "userId": random.choice(users),
        "is_fraud": random.choice(is_fraud),
        "cardType": random.choice(card_types),
        "merchant": random.choice(merchants),
        "items": random.randint(1, 5),          # number of items in transaction
        "paymentMethod": random.choice(["chip", "magstripe", "contactless"])
    }

while True:
    txn = generate_txn()
    print("Sending:", txn)
    producer.send("transactions", key=txn["transactionId"], value=txn)
    time.sleep(0.01)  # slightly faster sending
