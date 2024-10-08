CREATE TABLE IF NOT EXISTS `{dataset}.users` (
    user_id STRING NOT NULL,
    user_name STRING,
    date_of_birth DATE,
    sign_up_date DATE,
    updated_at TIMESTAMP
);