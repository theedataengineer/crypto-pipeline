-- fct_crypto_candles.sql
-- The primary fact table for price analysis.
-- Combines cleaned kline data with enriched price metrics.
--
-- In production: this table powers price charts, trend lines,
-- moving average overlays and volatility indicators on dashboards.
-- Materialized as a TABLE (not view) so Grafana queries are fast.

with price_metrics as (
    select * from {{ ref('int_price_metrics') }}
),

final as (
    select
        -- ─── Identity ──────────────────────────────────────────────
        kline_id,
        symbol,

        -- Extract date parts for easy filtering in dashboards
        opened_at,
        closed_at,
        date(opened_at)                                 as trade_date,
        extract(hour from opened_at)::integer           as trade_hour,
        to_char(opened_at, 'Day')                       as day_of_week,

        -- ─── Price OHLCV ───────────────────────────────────────────
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        number_of_trades,
        price_range,
        candle_direction,
        body_pct_of_range,

        -- ─── Returns & Momentum ────────────────────────────────────
        prev_close_price,
        pct_return,

        -- Classify the return size — useful for alert thresholds
        case
            when abs(pct_return) >= 3    then 'high_move'
            when abs(pct_return) >= 1    then 'medium_move'
            when abs(pct_return) >= 0.1  then 'small_move'
            else                              'flat'
        end                                             as move_category,

        -- ─── Moving Averages ───────────────────────────────────────
        ma_7,
        ma_14,
        ma_30,

        -- Golden/death cross signal
        -- Golden cross (ma_7 > ma_14) = bullish momentum signal
        -- Death cross  (ma_7 < ma_14) = bearish momentum signal
        case
            when ma_7 > ma_14 then 'golden_cross'
            when ma_7 < ma_14 then 'death_cross'
            else                    'neutral'
        end                                             as ma_signal,

        -- ─── Volatility & Volume ───────────────────────────────────
        volatility_7,
        avg_volume_7,
        volume_ratio,

        -- Flag unusual volume spikes — high volume = market attention
        case
            when volume_ratio >= 2.0 then 'high_volume_spike'
            when volume_ratio >= 1.5 then 'elevated_volume'
            else                          'normal_volume'
        end                                             as volume_signal,

        -- ─── Rankings ──────────────────────────────────────────────
        price_rank

    from price_metrics
)

select * from final
