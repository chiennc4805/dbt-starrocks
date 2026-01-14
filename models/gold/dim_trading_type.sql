SELECT DISTINCT
    trading_product_type,
    product_group
FROM {{source('hive_catalog_silver', 'src_txn_event')}}