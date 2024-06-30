import json
from confluent_kafka import Consumer, KafkaError
from collections import deque
from statistics import mean


conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'stock_group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['aapl-stock-topic'])



def consume_and_process():
    window_size_avg = 10
    window_size_sum = 5
    window_size_max = 30
    recent_v = deque(maxlen=window_size_avg)
    recent_n = deque(maxlen=window_size_sum)
    recent_o = deque(maxlen=window_size_max)

 
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            
            record = msg.value()
            record = json.loads(record)
            
            print(record)
            
            datetime, v, vw, o, c, h, l, t, n = record
    
               # Query 1: Average of v
            recent_v.append(v)
            if len(recent_v) == window_size_avg:
                avg_v = mean(recent_v)
                with open('average.txt', 'a') as f:
                    f.write(f'{avg_v}\n')
                    print(f'Updated average of v:  {avg_v}')
                
                # Query 2: Sum of n
                recent_n.append(n)
                if len(recent_n) == window_size_sum:
                    sum_n = sum(recent_n)
                    with open('sum.txt', 'a') as f:
                        f.write(f'{sum_n}\n')
                    print(f'Updated sum of n: {sum_n}')
                
                # Query 3: Highest value of o
                recent_o.append(o)
                if len(recent_o) == window_size_max:
                    max_o = max(recent_o)
                    with open('highest_value.txt', 'a') as f:
                        f.write(f'{max_o}\n')
                    print(f'Updated highest of o: {max_o}')
            

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    consume_and_process()
