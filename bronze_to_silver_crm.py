import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
 
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
 
BUCKET = 's3://customer-360-project'
 
# Read Bronze CRM CSV
crm_df = glueContext.create_dynamic_frame.from_catalog(
    database='customer360_db',
    table_name='bronze_salesforce_crm'
).toDF()
 
# Clean and transform
silver_crm = crm_df \
    .dropDuplicates(['customer_id']) \
    .filter(F.col('customer_id').isNotNull()) \
    .filter(F.col('email').isNotNull()) \
    .withColumn('email', F.lower(F.trim(F.col('email')))) \
    .withColumn('lifetime_value', F.col('lifetime_value').cast('double')) \
    .withColumn('last_contact_date', F.to_date('last_contact_date')) \
    .withColumn('processed_at', F.current_timestamp())
 
# Write to Silver
silver_crm.write.format('delta').mode('overwrite').save(f'{BUCKET}/silver/customers/')
 
print(f'Silver CRM written: {silver_crm.count()} rows')
job.commit()
