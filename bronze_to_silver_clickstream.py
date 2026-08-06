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
 
# Read Bronze clickstream
clicks_df = glueContext.create_dynamic_frame.from_catalog(
    database='customer360_db',
    table_name='bronze_clickstream'
).toDF()
 
# Clean and transform
silver_clicks = clicks_df \
    .dropDuplicates(['event_id']) \
    .filter(F.col('customer_id').isNotNull()) \
    .filter(F.col('event_type').isNotNull()) \
    .withColumn('event_ts', F.to_timestamp('timestamp')) \
    .withColumn('event_date', F.to_date('timestamp')) \
    .withColumn('event_hour', F.hour(F.to_timestamp('timestamp'))) \
    .withColumn('processed_at', F.current_timestamp())
 
# Write to Silver
silver_clicks.write.format('delta').mode('overwrite').partitionBy('event_date').save(f'{BUCKET}/silver/clickstream/')
 
print(f'Silver clickstream written: {silver_clicks.count()} rows')
job.commit()
