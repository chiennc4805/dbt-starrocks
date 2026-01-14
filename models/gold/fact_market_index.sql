WITH rnb_cte AS (
    SELECT 
        ts_5m_local,
        trade_date,
        index_code,
        index_name,
        is_active,
        index_value,
        ROW_NUMBER() OVER(PARTITION BY index_code, trade_date ORDER BY ts_5m_local DESC) AS rnb_ts_desc
    FROM {{source('hive_catalog_silver', 'src_index_5min')}}
),
last_day_value AS (
    SELECT
        *
    FROM rnb_cte
    WHERE rnb_ts_desc = 1
)

SELECT
    trade_date,
    index_code,
    index_value,
    LAG(index_value, 1, NULL) OVER(PARTITION BY index_code ORDER BY trade_date ASC) AS index_value_close_prev,
    ABS(index_value - index_value_close_prev) AS  abs_change,
    (index_value - index_value_close_prev) / index_value_close_prev AS pct_change
FROM last_day_value
