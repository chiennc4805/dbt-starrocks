
SELECT DISTINCT
    account_id,
    account_channel,
    account_branch
FROM {{source('hive_catalog_silver', 'src_txn_event')}}