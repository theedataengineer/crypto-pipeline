-- int_symbol_volume.sql
-- Aggregates trading volume and activity per symbol.
--
-- Real-world use: volume analysis tells you where market
-- participants are putting their money. High volume validates
-- price moves. Low volume moves are often false signals.

with trades as (
    select * from {{ ref('stg_binance_trades') }}
),

tickers as (
    select * from {{ ref('stg_binance_tickers') }}
),

-- Aggregate trade-level data by symbol
trade_summary as (
    select
        symbol,

        count(*)                                        as total_trades,
        round(sum(trade_value_usdt), 2)                 as total_volume_usdt,
        round(avg(trade_value_usdt), 4)                 as avg_trade_value_usdt,
        round(min(price), 4)                            as min_trade_price,
        round(max(price), 4)                            as max_trade_price,

        -- Buy vs sell pressure
        -- In real trading, buy pressure > sell pressure often
        -- signals upward price movement
        count(case when trade_direction = 'buy'  then 1 end) as buy_trades,
        count(case when trade_direction = 'sell' then 1 end) as sell_trades,

        round(
            count(case when trade_direction = 'buy' then 1 end)::numeric
            / nullif(count(*), 0) * 100
        , 2)                                            as buy_pressure_pct,

        sum(case when trade_direction = 'buy'
            then trade_value_usdt else 0 end)           as buy_volume_usdt,
        sum(case when trade_direction = 'sell'
            then trade_value_usdt else 0 end)           as sell_volume_usdt

    from trades
    group by symbol
),

-- Enrich with 24hr ticker context
enriched as (
    select
        ts.symbol,

        -- Trade activity
        ts.total_trades,
        ts.total_volume_usdt,
        ts.avg_trade_value_usdt,
        ts.min_trade_price,
        ts.max_trade_price,
        ts.buy_trades,
        ts.sell_trades,
        ts.buy_pressure_pct,
        ts.buy_volume_usdt,
        ts.sell_volume_usdt,

        -- 24hr market context from tickers
        tk.last_price,
        tk.price_change,
        tk.price_change_pct,
        tk.high_price                                   as daily_high,
        tk.low_price                                    as daily_low,
        tk.volume                                       as daily_volume,
        tk.quote_volume                                 as daily_quote_volume,
        tk.trade_count                                  as daily_trade_count,
        tk.price_spread_pct,

        -- Volume dominance: what % of total volume does this
        -- symbol represent across all our tracked pairs?
        round(
            ts.total_volume_usdt / nullif(sum(ts.total_volume_usdt)
                over (), 0) * 100
        , 2)                                            as volume_dominance_pct,

        -- Rank symbols by volume — most traded = rank 1
        rank() over (
            order by ts.total_volume_usdt desc
        )                                               as volume_rank,

        -- Rank by price change — biggest mover = rank 1
        rank() over (
            order by tk.price_change_pct desc
        )                                               as momentum_rank

    from trade_summary ts
    left join tickers tk using (symbol)
)

select * from enriched
