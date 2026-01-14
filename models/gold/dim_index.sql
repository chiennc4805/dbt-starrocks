SELECT DISTINCT
    index_code,
    index_name
FROM {{source('hive_catalog_silver', 'src_index_5min')}}