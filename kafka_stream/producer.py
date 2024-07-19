import json
import random
import time

from faker import Faker
from confluent_kafka import SerializingProducer
from datetime import datetime, timedelta

fake = Faker()

def generate_sales_transactions(start_time, end_time):
    user = fake.simple_profile()
    
    # Generate a random transaction time between start_time and end_time
    transaction_time = start_time + timedelta(seconds=random.randint(0, int((end_time - start_time).total_seconds())))
    
    return {
        "transactionId": fake.uuid4(),
        "productId": random.choice(['product1', 'product2', 'product3', 'product4', 'product5', 'product6']),
        "productName": random.choice(['laptop', 'mobile', 'tablet', 'watch', 'headphone', 'speaker']),
        'productCategory': random.choice(['electronic', 'fashion', 'grocery', 'home', 'beauty', 'sports']),
        'productPrice': round(random.uniform(10, 1000), 2),
        'productQuantity': random.randint(1, 10),
        'productBrand': random.choice(['apple', 'samsung', 'oneplus', 'mi', 'boat', 'sony']),
        'currency': random.choice(['USD', 'GBP']),
        'customerId': user['username'],
        'transactionDate': transaction_time.strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
        "paymentMethod": random.choice(['credit_card', 'debit_card', 'online_transfer'])
        }

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f"Message delivered to {msg.topic} [{msg.partition()}]")

def main():
    topic = 'financialtransactions'
    producer = SerializingProducer({
        'bootstrap.servers': 'localhost:29092,localhost:29093'
    })

    end_time = datetime.now()
    start_time = end_time - timedelta(days=7)
    
    curr_time = datetime.now()
    run_duration = timedelta(seconds=3600)  
    stop_time = curr_time + run_duration

    while datetime.now() < stop_time:
        try:
            transaction = generate_sales_transactions(start_time, end_time)
            print(transaction)

            producer.produce(topic,
                             key=transaction['transactionId'],
                             value=json.dumps(transaction),
                             on_delivery=delivery_report
                             )
            producer.poll(0)

            # Wait for 1 seconds before sending the next transaction
            time.sleep(1)
        except BufferError:
            print("Buffer full! Waiting...")
            time.sleep(1)
        except Exception as e:
            print(e)

if __name__ == "__main__":
    main()
