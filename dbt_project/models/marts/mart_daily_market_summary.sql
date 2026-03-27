-- mart_daily_market_summary.sql
-- Pre-aggregated daily market summary per symbol.
--
-- In production: this is the model your CEO looks at.
-- Pre-aggregating here means Grafana queries run in milliseconds
-- instead of seconds — critical for dashboard performance.
--
-- Answers:
--   What was the daily average price of each coin?
--   Which coin had the highest volume?
--   What was the daily price volatility?
--   What is the 7-day rolling average price trend?

with candles as (
    select * from {{ ref('fct_crypto_candles') }}
),

daily as (
    select
        trade_date,
        symbol,

        -- ─── Daily Price Summary ───────────────────────────────────
        round(min(low_price),   4)                      as daily_low,
        round(max(high_price),  4)                      as daily_high,
        round(avg(close_price), 4)                      as daily_avg_price,

        -- First and last close of the day
        first_value(open_price) over (
            partition by trade_date, symbol
            order by opened_at
            rows between unbounded preceding and unbounded following
        )                                               as daily_open,

        last_value(close_price) over (
            partition by trade_date, symbol
            order by opened_at
            rows between unbounded preceding and unbounded following
        )                                               as daily_close,

        -- ─── Daily Volume ──────────────────────────────────────────
        round(sum(volume),          4)                  as daily_volume,
        sum(number_of_trades)                           as daily_trades,
        count(*)                                        as total_candles,

        -- ─── Volatility ────────────────────────────────────────────
        -- High-Low range as % of daily open = daily volatility score
        round(
            (max(high_price) - min(low_price))
            / nullif(min(low_price), 0) * 100
        , 4)                                            as daily_volatility_pct,

        round(avg(volatility_7), 4)                     as avg_intraday_volatility,

        -- ─── Candle Direction Counts ───────────────────────────────
        count(case when candle_direction = 'bullish' then 1 end) as bullish_candles,
        count(case when candle_direction = 'bearish' then 1 end) as bearish_candles,

        -- ─── Volume Signals ────────────────────────────────────────
        count(case when volume_signal = 'high_volume_spike'  then 1 end) as volume_spikes,
        count(case when volume_signal = 'elevated_volume'    then 1 end) as elevated_volume_candles,

        -- ─── MA Signals ────────────────────────────────────────────
        -- How many hours was this coin in golden vs death cross?
        count(case when ma_signal = 'golden_cross' then 1 end) as golden_cross_hours,
        count(case when ma_signal = 'death_cross'  then 1 end) as death_cross_hours,

        -- ─── Average MAs for the day ───────────────────────────────
        round(avg(ma_7),  4)                            as avg_ma_7,
        round(avg(ma_14), 4)                            as avg_ma_14,
        round(avg(ma_30), 4)                            as avg_ma_30

    from candles
    group by trade_date, symbol, opened_at, open_price, close_price
),

-- Deduplicate the window function results
deduped as (
    select distinct
        trade_date,
        symbol,
        daily_low,
        daily_high,
        daily_avg_price,
        daily_open,
        daily_close,
        daily_volume,
        daily_trades,
        total_candles,
        daily_volatility_pct,
        avg_intraday_volatility,
        bullish_candles,
        bearish_candles,
        volume_spikes,
        elevated_volume_candles,
        golden_cross_hours,
        death_cross_hours,
        avg_ma_7,
        avg_ma_14,
        avg_ma_30,

        -- Daily return: how much did price change open to close?
        round(
            (daily_close - daily_open)
            / nullif(daily_open, 0) * 100
        , 4)                                            as daily_return_pct,

        -- 7-day rolling average price — the trend line
        round(avg(daily_avg_price) over (
            partition by symbol
            order by trade_date
            rows between 6 preceding and current row
        ), 4)                                           as rolling_7d_avg_price,

        -- Day-over-day volume change
        round(
            (daily_volume - lag(daily_volume) over (
                partition by symbol order by trade_date
            )) / nullif(lag(daily_volume) over (
                partition by symbol order by trade_date
            ), 0) * 100
        , 2)                                            as volume_change_pct

    from daily
)

select * from deduped
order by trade_date desc, symbol
