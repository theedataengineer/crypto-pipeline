-- stg_binance_trades.sql
-- Cleans individual trade transaction records.
-- Key jobs: convert timestamps, calculate trade value,
-- make buyer/seller direction human-readable.

with source as (
    select * from {{ source('binance_raw', 'raw_binance_trades') }}
),

cleaned as (
    select
        -- Surrogate key
        symbol || '_' || cast(trade_id as text)     as trade_key,

        symbol,
        trade_id,

        -- Convert Unix ms to timestamp
        to_timestamp(trade_time / 1000.0)           as traded_at,

        price::numeric                              as price,
        quantity::numeric                           as quantity,
        quote_qty::numeric                          as quote_qty,

        -- Total trade value in USDT
        price::numeric * quantity::numeric          as trade_value_usdt,

        -- Human readable direction
        case
            when is_buyer_maker = true then 'sell'
            else 'buy'
        end                                         as trade_direction,

        extracted_at::timestamp                     as extracted_at

    from source
),

deduplicated as (
    select *,
        row_number() over (
            partition by trade_key
            order by extracted_at desc
        ) as row_num
    from cleaned
)

select * from deduplicated
where row_num = 1
