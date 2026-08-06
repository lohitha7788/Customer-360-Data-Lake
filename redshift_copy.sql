-- Load Gold layer Parquet into Redshift
COPY customer360.customer_360
FROM 's3://customer-360-project/gold/customer-360-export/'
IAM_ROLE 'arn:aws:iam::397572991099:role/redshift-s3-role'
FORMAT AS PARQUET;

-- Verify row count
SELECT COUNT(*) AS total_customers FROM customer360.customer_360;
 
-- Quick sanity check
SELECT churn_risk, COUNT(*) AS customer_count
FROM customer360.customer_360
GROUP BY churn_risk ORDER BY customer_count DESC;

SELECT customer_id, segment, churn_risk, rfm_segment, recency_days, frequency, monetary_value
FROM customer360.customer_360
LIMIT 10;