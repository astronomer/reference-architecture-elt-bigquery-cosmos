CREATE TABLE IF NOT EXISTS `{dataset}.utms` (
    utm_id STRING NOT NULL,
    utm_source STRING,
    utm_medium STRING,
    utm_campaign STRING,
    utm_term STRING,
    utm_content STRING,
    updated_at TIMESTAMP
);