-- stg_binance_tickers.sql
-- Cleans 24hr market ticker statistics.

with source as (
    select * from {{ source('binance_raw', 'raw_binance_tickers') }}
),

cleaned as (
    select
        symbol,
        price_change::numeric                       as price_change,
        price_change_pct::numeric                   as price_change_pct,
        weighted_avg_price::numeric                 as weighted_avg_price,
        prev_close_price::numeric                   as prev_close_price,
        last_price::numeric                         as last_price,
        open_price::numeric                         as open_price,
        high_price::numeric                         as high_price,
        low_price::numeric                          as low_price,
        volume::numeric                             as volume,
        quote_volume::numeric                       as quote_volume,
        count                                       as trade_count,

        to_timestamp(open_time  / 1000.0)           as period_start,
        to_timestamp(close_time / 1000.0)           as period_end,

        -- Price spread as % of open price
        round(
            (high_price::numeric - low_price::numeric)
            / nullif(open_price::numeric, 0) * 100, 4
        )                                           as price_spread_pct,

        extracted_at::timestamp                     as extracted_at

    from source
)

select * from cleaned
