import pandas as pd
from kafka import KafkaProducer
import json
import time
import random

# Initialize the 'Conveyor Belt' connection
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Load your local data
df = pd.read_csv('dataset/creditcard.csv')

print("Starting transaction stream...")
counter = 0
try:
    while True:
        # Pick a random transaction to simulate a 'live' event
        dict_stock = df.sample(1).to_dict(orient="records")[0]
        
        # Send it to the 'transactions' topic
        producer.send('transactions', dict_stock)
        print(f"Sent: Amount ${dict_stock['Amount']}")
        # Inside your Producer loop
        #if counter > 0 and counter % 10 == 0:
        # Specifically pick a row where Class is 1 (Fraud)
            #fraud_sample = df[df['Class'] == 1].sample(1).to_dict(orient="records")[0]
            #producer.send('transactions', fraud_sample)
            #print("🔥 Sent a KNOWN FRAUD row for testing")
                # Wait a bit so it's not too fast
        time.sleep(random.uniform(1, 3))
except KeyboardInterrupt:
    print("Stream stopped.")