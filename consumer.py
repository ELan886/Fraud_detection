from kafka import KafkaConsumer
import json
import joblib
import pandas as pd

# 1. Load your trained assets
pipeline = joblib.load('fraud_pipeline.pkl')


# 2. Connect to the Kafka stream
consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Security Guard is active. Monitoring transactions...")

for message in consumer:
    tx = message.value
    
    # 3. Format data for the model
    # Note: Ensure features are in the SAME order as your X_train columns!
    current_df = pd.DataFrame([tx])
    
    # Scale 'Time' and 'Amount' just like we did in Phase 1
    prob_fraud = pipeline.predict_proba(current_df)[0][1]
    
    if prob_fraud > 0.8:
        print(f"!!! FRAUD ALERT !!! Amount: ${tx['Amount']} | Probability: {prob_fraud:.2f}")
    else:
        print(f"Approved: ${tx['Amount']}")