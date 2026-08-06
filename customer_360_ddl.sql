CREATE SCHEMA IF NOT EXISTS customer360;
 
CREATE TABLE IF NOT EXISTS customer360.customer_360 (
    customer_id     VARCHAR(20)     NOT NULL,
    first_name      VARCHAR(100),
    last_name       VARCHAR(100),
    email           VARCHAR(200),
    segment         VARCHAR(50),
    lifetime_value  DECIMAL(12,2),
    recency_days    INT,
    frequency       INT,
    monetary_value  DECIMAL(12,2),
    return_count    INT,
    total_clicks    INT,
    total_sessions  INT,
    active_days     INT,
    churn_risk      VARCHAR(10),
    rfm_segment     VARCHAR(50),
    last_order_date TIMESTAMP,
    updated_at      TIMESTAMP
)
DISTKEY(customer_id)
SORTKEY(churn_risk, rfm_segment);