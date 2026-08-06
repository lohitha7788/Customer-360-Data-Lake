import psycopg2, random
from datetime import datetime, timedelta
from faker import Faker
 
fake = Faker()
 
# Replace with your RDS endpoint from console
conn = psycopg2.connect(
    host='customer360-postgres.c7m4eca22iui.ca-central-1.rds.amazonaws.com',
    database='customer360',
    user='postgres',
    password='A778800#a'
)
cur = conn.cursor()
 
# Create tables
cur.execute('''
CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(20) PRIMARY KEY,
    email VARCHAR(100),
    signup_date DATE,
    country VARCHAR(100)
);
''')
 
cur.execute('''
CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20),
    order_date TIMESTAMP,
    total_amount DECIMAL(10,2),
    status VARCHAR(20)
);
''')
 
cur.execute('''
CREATE TABLE IF NOT EXISTS products (
    product_id VARCHAR(20) PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    price DECIMAL(10,2)
);
''')
 
# Load 500 sample customers
for i in range(1, 501):
    cur.execute('INSERT INTO customers VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING',
        (f'CUST_{i:04d}', fake.email(),
         fake.date_between('-2y', 'today'), fake.country()))
 
# Load 2000 sample orders
for i in range(1, 2001):
    cur.execute('INSERT INTO orders VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING',
        (f'ORD_{i:06d}', f'CUST_{random.randint(1,500):04d}',
         fake.date_time_between('-1y', 'now'),
         round(random.uniform(10, 500), 2),
         random.choice(['completed','pending','returned'])))
 
# Load 100 sample products
for i in range(1, 101):
    cur.execute('INSERT INTO products VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING',
        (f'PROD_{i:04d}', fake.bs(),
         random.choice(['Electronics','Clothing','Home','Sports']),
         round(random.uniform(5, 300), 2)))
 
conn.commit()
cur.close()
conn.close()
print('Sample data loaded successfully')
