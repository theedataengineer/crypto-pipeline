-- dim_symbols.sql
-- Dimension table describing each trading pair.

with tickers as (
    select * from {{ ref('stg_binance_tickers') }}
),

kline_stats as (
    select
        symbol,
        min(opened_at)                                  as first_seen_at,
        max(opened_at)                                  as last_seen_at,
        count(*)                                        as total_candles,
        round(min(close_price), 4)                      as historical_low,
        round(max(close_price), 4)                      as historical_high,
        round(avg(close_price), 4)                      as avg_close_price
    from {{ ref('stg_binance_klines') }}
    group by symbol
),

final as (
    select
        -- ─── Identity ──────────────────────────────────────────────
        t.symbol,

        -- Parse base and quote asset from symbol name
        left(t.symbol, length(t.symbol) - 4)           as base_asset,
        right(t.symbol, 4)                             as quote_asset,

        -- ─── Current Market State ──────────────────────────────────
        t.last_price                                   as current_price,
        t.price_change,
        t.price_change_pct,

        -- ✅ FIXED: map actual staging columns to business names
        t.high_price        as daily_high,
        t.low_price         as daily_low,
        t.volume            as daily_volume,
        t.quote_volume      as daily_quote_volume,
        t.trade_count       as daily_trade_count,

        t.price_spread_pct,

        -- Market sentiment
        case
            when t.price_change_pct > 5    then 'strongly_bullish'
            when t.price_change_pct > 1    then 'bullish'
            when t.price_change_pct > -1   then 'neutral'
            when t.price_change_pct > -5   then 'bearish'
            else                                'strongly_bearish'
        end                                             as market_sentiment,

        -- ─── Historical Context ────────────────────────────────────
        k.first_seen_at,
        k.last_seen_at,
        k.total_candles,
        k.historical_low,
        k.historical_high,
        k.avg_close_price,

        -- Distance from historical high
        round(
            (t.last_price - k.historical_high)
            / nullif(k.historical_high, 0) * 100
        , 2)                                            as pct_from_high,

        -- ─── Metadata ──────────────────────────────────────────────
        current_timestamp                               as last_updated_at

    from tickers t
    left join kline_stats k using (symbol)
)

select * from final
