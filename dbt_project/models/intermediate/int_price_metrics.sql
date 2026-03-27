-- int_price_metrics.sql
-- Enriches cleaned kline data with price analytics.
--
-- Real-world use: traders and analysts use these metrics to
-- identify trends, measure risk, and build trading signals.
-- Window functions are the core tool here — learn them well.

with klines as (
    select * from {{ ref('stg_binance_klines') }}
),

price_metrics as (
    select
        kline_id,
        symbol,
        opened_at,
        closed_at,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        number_of_trades,
        candle_direction,
        price_range,

        -- ─── Returns ──────────────────────────────────────────────
        -- How much did price move vs the previous candle?
        -- This is the foundation of momentum and trend analysis.
        lag(close_price) over (
            partition by symbol
            order by opened_at
        )                                               as prev_close_price,

        round(
            (close_price - lag(close_price) over (
                partition by symbol order by opened_at
            )) / nullif(lag(close_price) over (
                partition by symbol order by opened_at
            ), 0) * 100, 4
        )                                               as pct_return,

        -- ─── Moving Averages ───────────────────────────────────────
        -- Smooth out noise to reveal the underlying trend.
        -- 7-period MA on hourly data = 7-hour moving average.
        round(avg(close_price) over (
            partition by symbol
            order by opened_at
            rows between 6 preceding and current row
        ), 4)                                           as ma_7,

        round(avg(close_price) over (
            partition by symbol
            order by opened_at
            rows between 13 preceding and current row
        ), 4)                                           as ma_14,

        round(avg(close_price) over (
            partition by symbol
            order by opened_at
            rows between 29 preceding and current row
        ), 4)                                           as ma_30,

        -- ─── Volatility ────────────────────────────────────────────
        -- Standard deviation of returns over a rolling window.
        -- High volatility = high risk and high opportunity.
        round(stddev(close_price) over (
            partition by symbol
            order by opened_at
            rows between 6 preceding and current row
        ), 4)                                           as volatility_7,

        -- ─── Volume Metrics ────────────────────────────────────────
        -- Is this candle's volume unusual compared to recent history?
        -- Unusual volume often precedes big price moves.
        round(avg(volume) over (
            partition by symbol
            order by opened_at
            rows between 6 preceding and current row
        ), 4)                                           as avg_volume_7,

        round(volume / nullif(avg(volume) over (
            partition by symbol
            order by opened_at
            rows between 6 preceding and current row
        ), 0), 4)                                       as volume_ratio,

        -- ─── Candle Body Size ──────────────────────────────────────
        -- Large body = strong conviction. Small body = indecision.
        round(
            abs(close_price - open_price) /
            nullif(price_range, 0) * 100
        , 4)                                            as body_pct_of_range,

        -- ─── Rank within snapshot ──────────────────────────────────
        -- Which coin had the highest close price this candle?
        rank() over (
            partition by opened_at
            order by close_price desc
        )                                               as price_rank

    from klines
)

select * from price_metrics
