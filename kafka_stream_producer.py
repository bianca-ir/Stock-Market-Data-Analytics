import json
from confluent_kafka import Producer
from datetime import datetime, timedelta
import time

conf = {
    'bootstrap.servers': 'localhost:9092'
}

producer = Producer(conf)

def parse_json(data):
    current_time = datetime.now()
    tuples = []
    for i, result in enumerate(data["results"]):
        timestamp = (current_time + timedelta(minutes=i)).strftime("%H:%M:%S")
        tuple_data = (timestamp, result["v"], result["vw"], result["o"], result["c"], result["h"], result["l"], result["t"], result["n"])
        tuples.append(tuple_data)
    return tuples

def delivery_report(err, msg):
    if err is not None:
       print('Message delivery failed: {}'.format(err))
    else:
        print('Message was sent to {} [{}]'.format(msg.topic(), msg.partition()))

def send_data():
    with open('aapl_stock_market.json', 'r') as f:
        data = json.load(f) 
        tuples = parse_json(data) 
        for record in tuples:
         
            producer.produce('aapl-stock-topic', json.dumps(record).encode('utf-8'), callback=delivery_report)      
            producer.poll(0)
            time.sleep(1) 

    producer.flush()

if __name__ == "__main__":
    send_data()
