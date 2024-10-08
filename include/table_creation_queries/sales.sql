CREATE TABLE IF NOT EXISTS `{dataset}.sales` (
    sale_id STRING NOT NULL,
    user_id STRING NOT NULL,
    cheese_id STRING NOT NULL,
    utm_id STRING NOT NULL,
    quantity INT64,
    sale_date TIMESTAMP
);