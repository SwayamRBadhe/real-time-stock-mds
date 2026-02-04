/* these are views to create the candlestick data for each stock symbol. candlestick data is used to visualize the price movements of stocks over a specific time period. each candlestick represents the open, high, low, and close prices for a given time period. in this model we are creating daily candlesticks for the last 12 days for each stock symbol based on the cleaned data from silver layer. we are also calculating a trend line which is the average price for each day. this view can be used in power bi to create candlestick charts for visualizing stock price movements.
*/

with enriched as ( --enriching the silver layer data with trade_date, candle_open and candle_close using window functions
    select
        symbol,
        cast(market_timestamp as date) as trade_date, --extracting date part from market_timestamp
        day_low,
        day_high,
        current_price,
        first_value(current_price) over ( --getting the opening price of the day using first_value window function
            partition by symbol, cast(market_timestamp as date) --partitioning by symbol and trade_date. partion means for each symbol and trade_date we will calculate the first_value
            order by market_timestamp --ordering by market_timestamp to get the first value
        ) as candle_open,
        last_value(current_price) over ( --getting the closing price of the day using last_value window function
            partition by symbol, cast(market_timestamp as date) --partitioning by symbol and trade_date
            order by market_timestamp
            rows between unbounded preceding and unbounded following --this is required to get the last value in the partition
        ) as candle_close
    from {{ ref('silver_clean_stock_quotes') }} --referring to silver layer cleaned stock quotes data
),

candles as ( --aggregating the enriched data to get daily candlestick data
    select
        symbol,
        trade_date as candle_time, --renaming trade_date to candle_time
        min(day_low) as candle_low, --getting the low price of the day
        max(day_high) as candle_high, --getting the high price of the day
        any_value(candle_open) as candle_open, --getting the opening price of the day
        any_value(candle_close) as candle_close, --getting the closing price of the day
        avg(current_price) as trend_line -- calculating the average price of the day as trend line
    from enriched
    group by symbol, trade_date
),

ranked as ( --ranking the candles to get last 12 days of candlestick data per symbol
    select
        c.*, --selecting all columns from candles
        row_number() over ( --using row_number window function to rank the candles
            partition by symbol
            order by candle_time desc
        ) as rn
    from candles c
)

select
    symbol,
    candle_time,
    candle_low,
    candle_high,
    candle_open,
    candle_close,
    trend_line
from ranked
where rn <= 12
order by symbol, candle_time

/* in short and simple terms this view creates daily candlestick data for each stock symbol for the last 12 days along with a trend line which is the average price for each day. this data can be used to visualize stock price movements in power bi using candlestick charts.
*/
