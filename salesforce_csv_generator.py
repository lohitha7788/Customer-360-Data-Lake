import pandas as pd, boto3, random
from datetime import datetime
from faker import Faker
 
fake = Faker()
BUCKET = 'customer-360-project'
 
records = []
for i in range(1, 501):
    records.append({
        'customer_id': f'CUST_{i:04d}',
        'first_name': fake.first_name(),
        'last_name': fake.last_name(),
        'email': fake.email(),
        'phone': fake.phone_number(),
        'segment': random.choice(['VIP','Regular','At-Risk','New']),
        'lifetime_value': round(random.uniform(0, 5000), 2),
        'last_contact_date': fake.date_between('-6m', 'today').isoformat(),
        'opted_in_marketing': random.choice([True, False]),
        'export_date': datetime.utcnow().strftime('%Y-%m-%d')
    })
 
df = pd.DataFrame(records)
filename = f'salesforce_export_{datetime.utcnow().strftime("%Y%m%d")}.csv'
df.to_csv(filename, index=False)
 
s3 = boto3.client('s3', region_name='ca-central-1')
s3.upload_file(filename, BUCKET, f'bronze/salesforce-crm/{filename}')
print(f'Uploaded {filename} to S3')
