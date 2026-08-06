import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType, IntegerType

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

BUCKET = 's3://customer-360-project'

# Read Silver tables (Delta format)
orders_df = spark.read.format('delta').load(f'{BUCKET}/silver/orders/')
clicks_df = spark.read.format('delta').load(f'{BUCKET}/silver/clickstream/')
crm_df = spark.read.format('delta').load(f'{BUCKET}/silver/customers/')

# RFM features from orders
rfm_df = orders_df.groupBy('customer_id').agg(
    F.datediff(F.current_date(), F.max('order_date')).alias('recency_days'),
    F.count('order_id').alias('frequency'),
    F.round(F.sum('total_amount'), 2).alias('monetary_value'),
    F.max('order_date').alias('last_order_date'),
    F.min('order_date').alias('first_order_date'),
    F.countDistinct(F.when(F.col('status') == 'RETURNED',
        F.col('order_id'))).alias('return_count')
)

# Click features — total clicks and unique sessions per customer
click_features = clicks_df.groupBy('customer_id').agg(
    F.count('event_id').alias('total_clicks'),
    F.countDistinct('session_id').alias('total_sessions'),
    F.countDistinct('event_date').alias('active_days')
)

# Join all three Silver tables
customer_360 = crm_df \
    .join(rfm_df, on='customer_id', how='left') \
    .join(click_features, on='customer_id', how='left')

# Compute churn risk score (simple rule-based)
customer_360 = customer_360.withColumn('churn_risk',
    F.when((F.col('recency_days') > 90) & (F.col('frequency') < 3), 'HIGH')
    .when((F.col('recency_days') > 60) & (F.col('frequency') < 5), 'MEDIUM')
    .otherwise('LOW')
)

# Add RFM segment label
customer_360 = customer_360.withColumn('rfm_segment',
    F.when(F.col('segment') == 'VIP', 'Champions')
    .when(F.col('churn_risk') == 'HIGH', 'At Risk')
    .when(F.col('frequency') >= 10, 'Loyal Customers')
    .when(F.col('recency_days') <= 30, 'Recent Customers')
    .otherwise('Needs Attention')
)

# Add processing timestamp
customer_360 = customer_360.withColumn('updated_at', F.current_timestamp())

# --- Validation: row counts ---
orders_count = orders_df.count()
clicks_count = clicks_df.count()
crm_count = crm_df.count()
gold_count = customer_360.count()

print(f'Silver row counts — orders: {orders_count}, clicks: {clicks_count}, crm: {crm_count}')
print(f'Gold row count — customer_360: {gold_count}')

if gold_count == 0:
    raise Exception('Validation failed: Gold table has 0 rows')

if gold_count > crm_count:
    raise Exception(f'Validation failed: Gold row count ({gold_count}) exceeds CRM source count ({crm_count}) — possible join fan-out')

# --- Validation: null rates on key columns ---
key_columns = ['customer_id', 'email', 'churn_risk', 'rfm_segment']
for col_name in key_columns:
    null_count = customer_360.filter(F.col(col_name).isNull()).count()
    null_rate = round((null_count / gold_count) * 100, 2) if gold_count > 0 else 0
    print(f'Null rate for {col_name}: {null_rate}% ({null_count}/{gold_count})')
    if col_name == 'customer_id' and null_count > 0:
        raise Exception(f'Validation failed: {null_count} rows have null customer_id')

# Write to Gold layer (Delta format — for the data lake, all columns)
customer_360.write.format('delta').mode('overwrite').save(f'{BUCKET}/gold/customer-360/')

# Build a Redshift-compatible export with only the 17 matching columns, in order,
# with explicit types matching the Redshift DDL exactly
redshift_export = customer_360.select(
    F.col('customer_id').cast('string'),
    F.col('first_name').cast('string'),
    F.col('last_name').cast('string'),
    F.col('email').cast('string'),
    F.col('segment').cast('string'),
    F.col('lifetime_value').cast(DecimalType(12, 2)).alias('lifetime_value'),
    F.col('recency_days').cast(IntegerType()).alias('recency_days'),
    F.col('frequency').cast(IntegerType()).alias('frequency'),
    F.col('monetary_value').cast(DecimalType(12, 2)).alias('monetary_value'),
    F.col('return_count').cast(IntegerType()).alias('return_count'),
    F.col('total_clicks').cast(IntegerType()).alias('total_clicks'),
    F.col('total_sessions').cast(IntegerType()).alias('total_sessions'),
    F.col('active_days').cast(IntegerType()).alias('active_days'),
    F.col('churn_risk').cast('string'),
    F.col('rfm_segment').cast('string'),
    F.col('last_order_date').cast('timestamp'),
    F.col('updated_at').cast('timestamp')
)

# Write to Gold layer (plain Parquet — for Redshift COPY compatibility)
redshift_export.write.mode('overwrite').parquet(f'{BUCKET}/gold/customer-360-export/')

print(f'Customer 360 Gold table written: {gold_count} customers')
job.commit()