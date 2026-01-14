WITH first_last_value_of_day AS (
    SELECT
        ts_5m_local,
        trade_date,
        stock_code,
        stock_name,
        primary_sector,
        listing_date,
        is_active,
        open_vnd,
        high_vnd,
        low_vnd,
        close_vnd,
        volume_shares,
        ROW_NUMBER() OVER(PARTITION BY stock_code, trade_date ORDER BY ts_5m_local ASC) AS rnb_ts_asc,
        ROW_NUMBER() OVER(PARTITION BY stock_code, trade_date ORDER BY ts_5m_local DESC) AS rnb_ts_desc
    FROM {{source('hive_catalog_silver', 'src_stock_5min')}}
),
calculated_cte AS(
    SELECT
        stock_code,
        trade_date,
        CASE rnb_ts_asc
            WHEN 1 THEN open_vnd
        END AS daily_open_price,
        CASE rnb_ts_desc
            WHEN 1 THEN AVG(high_vnd, low_vnd) 
        END AS price,
        MIN(low_vnd) AS daily_low_price,
        MAX(high_vnd) AS daily_high_price,
        SUM(volume_shares) AS trading_volume,
        SUM(AVG(closed_vnd) * SUM(volume_shares)) AS trading_value
    FROM first_last_value_of_day
    GROUP BY stock_code, trade_date
)

SELECT
    stock_code,
    trade_date,
    price,
    LAG(price, 1, NULL) OVER(PARTITION BY stock_code ORDER BY trade_date ASC) AS price_close_prev,
    daily_open_price,
    daily_low_price,
    daily_high_price,
    trading_volume,
    trading_value
FROM calculated_first_last_value