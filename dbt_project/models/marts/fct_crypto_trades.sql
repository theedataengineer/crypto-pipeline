-- fct_crypto_trades.sql
-- Fact table for individual trade transactions.
--
-- In production: used for microstructure analysis, detecting
-- large "whale" trades, and understanding buyer/seller dynamics.

with trades as (
    select * from {{ ref('stg_binance_trades') }}
),

final as (
    select
        -- ─── Identity ──────────────────────────────────────────────
        trade_key,
        symbol,
        trade_id,
        traded_at,
        date(traded_at)                                 as trade_date,
        extract(hour from traded_at)::integer           as trade_hour,

        -- ─── Trade Details ─────────────────────────────────────────
        price,
        quantity,
        quote_qty,
        trade_value_usdt,
        trade_direction,

        -- ─── Trade Size Classification ─────────────────────────────
        -- In crypto, large trades are called "whale" trades.
        -- Tracking these matters because whales move markets.
        case
            when trade_value_usdt >= 100000 then 'whale'
            when trade_value_usdt >= 10000  then 'large'
            when trade_value_usdt >= 1000   then 'medium'
            else                                 'small'
        end                                             as trade_size_category,

        -- ─── Rolling context ───────────────────────────────────────
        -- How does this trade compare to recent average trade size?
        round(
            trade_value_usdt / nullif(avg(trade_value_usdt) over (
                partition by symbol
                order by traded_at
                rows between 9 preceding and current row
            ), 0)
        , 4)                                            as relative_size,

        -- Cumulative volume per symbol — running total of activity
        sum(trade_value_usdt) over (
            partition by symbol
            order by traded_at
            rows between unbounded preceding and current row
        )                                               as cumulative_volume_usdt

    from trades
)

select * from final
