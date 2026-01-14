SELECT
    *
FROM {{source('hive_catalog_silver', 'src_customer_segment_monthly')}}