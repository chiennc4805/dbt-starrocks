SELECT
    account_id, 
    trading_product_type,
    trade_date,
    SUM(
        CASE event_type
            WHEN 'PLACED' THEN 1
            ELSE 0
        END
    ) AS orders_placed,
    SUM(
        CASE event_type
            WHEN 'MATCHED' THEN 1
            ELSE 0
        END
    ) AS orders_matched,
    SUM(
        CASE event_type
            WHEN 'CANCELED' THEN 1
            ELSE 0
        END
    ) AS orders_cancelled,
    SUM(matched_value_vnd) AS total_trading_value,
    SUM(trading_fee_vnd) AS total_trading_fee
FROM {{source('hive_catalog_silver', 'src_txn_event')}}
GROUP BY account_id, trading_product_type, trade_date