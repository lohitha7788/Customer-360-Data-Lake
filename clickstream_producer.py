import json, random, time, boto3
from datetime import datetime
 
firehose = boto3.client('firehose', region_name='ca-central-1')
STREAM = 'customer360-clickstream'
 
EVENTS = ['page_view','product_view','add_to_cart',
          'remove_from_cart','checkout','purchase','search']
PAGES = ['/home','/products','/cart','/checkout','/account','/search']
 
def generate_click_event():
    return {
        'event_id': f'EVT_{int(time.time()*1000)}_{random.randint(100,999)}',
        'customer_id': f'CUST_{random.randint(1,500):04d}',
        'event_type': random.choice(EVENTS),
        'page': random.choice(PAGES),
        'product_id': f'PROD_{random.randint(1,100):04d}',
        'session_id': f'SESS_{random.randint(10000,99999)}',
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'device': random.choice(['mobile','desktop','tablet']),
        'country': random.choice(['CA','US','GB','AU'])
    }
 
print('Starting clickstream producer...')
batch = []
while True:
    for _ in range(50):
        event = generate_click_event()
        batch.append({'Data': (json.dumps(event) + '\n').encode()})
    firehose.put_record_batch(DeliveryStreamName=STREAM, Records=batch)
    print(f'Sent 50 events | {datetime.utcnow().isoformat()}')
    batch = []
    time.sleep(1)
