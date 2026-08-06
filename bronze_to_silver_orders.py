import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import *
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

BUCKET = 's3://customer-360-project'

# Read Bronze orders
orders_df = glueContext.create_dynamic_frame.from_catalog(
    database='customer360_db',
    table_name='bronze_orders'
).toDF()

# Clean and transform
silver_orders = orders_df \
    .dropDuplicates(['order_id']) \
    .filter(F.col('order_id').isNotNull()) \
    .filter(F.col('customer_id').isNotNull()) \
    .filter(F.col('total_amount') > 0) \
    .withColumn('order_date', F.to_timestamp('order_date')) \
    .withColumn('total_amount', F.col('total_amount').cast(DoubleType())) \
    .withColumn('status', F.upper(F.trim(F.col('status')))) \
    .withColumn('order_year', F.year('order_date')) \
    .withColumn('order_month', F.month('order_date')) \
    .withColumn('processed_at', F.current_timestamp())

# --- Data Quality checks ---
silver_dyf = DynamicFrame.fromDF(silver_orders, glueContext, 'silver_orders')

dq_rules = '''
Rules = [
    IsComplete 'order_id',
    IsComplete 'customer_id',
    IsComplete 'order_date',
    ColumnValues 'total_amount' > 0,
    IsUnique 'order_id',
    ColumnValues 'status' in ['COMPLETED','PENDING','RETURNED']
]
'''

dq_results = glueContext.evaluate_data_quality(
    frame=silver_dyf,
    ruleset=dq_rules,
    publishing_options={
        'dataQualityEvaluationContext': 'orders-silver-dq',
        'enableDataQualityCloudWatchMetrics': True,
        'enableDataQualityResultsPublishing': True
    }
)

if dq_results.select(F.col('Failed')).collect()[0][0] > 0:
    raise Exception('Data quality check failed — see Glue Data Quality results')

# Write to Silver as Parquet
silver_orders.write \
    .format('delta') \
    .mode('overwrite') \
    .partitionBy('order_year', 'order_month') \
    .save(f'{BUCKET}/silver/orders/')

print(f'Silver orders written: {silver_orders.count()} rows')
job.commit()