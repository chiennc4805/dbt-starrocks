SELECT DISTINCT
    stock_code,
    stock_name,
    primary_sector,
    listing_date
FROM {{source('hive_catalog_silver', 'src_stock_5min')}}