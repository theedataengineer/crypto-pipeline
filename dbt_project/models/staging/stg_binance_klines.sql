-- stg_binance_klines.sql
-- Cleans and standardizes raw candlestick data.
-- Key jobs: convert Unix timestamps to readable timestamps,
-- cast text prices to numeric, add a unique surrogate key.

with source as (
    select * from {{ source('binance_raw', 'raw_binance_klines') }}
),

cleaned as (
    select
        -- Surrogate key: unique identifier for each candle
        symbol || '_' || cast(open_time as text)    as kline_id,

        symbol,

        -- Convert Unix milliseconds to proper timestamps
        to_timestamp(open_time  / 1000.0)           as opened_at,
        to_timestamp(close_time / 1000.0)           as closed_at,

        -- Prices cast to numeric for calculations
        open_price::numeric                         as open_price,
        high_price::numeric                         as high_price,
        low_price::numeric                          as low_price,
        close_price::numeric                        as close_price,
        volume::numeric                             as volume,
        quote_asset_volume::numeric                 as quote_asset_volume,
        number_of_trades::integer                   as number_of_trades,
        taker_buy_base_volume::numeric              as taker_buy_base_volume,
        taker_buy_quote_volume::numeric             as taker_buy_quote_volume,

        -- Price range for the candle
        high_price::numeric - low_price::numeric    as price_range,

        -- Was this a bullish (green) or bearish (red) candle?
        case
            when close_price::numeric >= open_price::numeric then 'bullish'
            else 'bearish'
        end                                         as candle_direction,

        extracted_at::timestamp                     as extracted_at

    from source
),

-- Remove duplicates — always do this in staging
deduplicated as (
    select *,
        row_number() over (
            partition by kline_id
            order by extracted_at desc
        ) as row_num
    from cleaned
)

select * from deduplicated
where row_num = 1
