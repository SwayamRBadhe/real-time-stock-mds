/* this view calculates key metrics for stock symbols such as average price for the latest trading day, all-time volatility, and relative volatility using cleaned stock quotes data from the silver layer. these metrics can help in analyzing stock performance and risk assessment. basically, average price gives an idea of the stock's value on the latest day, volatility indicates how much the stock price fluctuates over time, and relative volatility provides a normalized measure of volatility in relation to the stock's average price.
*/

WITH source AS ( --preparing the source data by casting current_price to double for calculations
  SELECT
    symbol,
    TRY_CAST(current_price AS DOUBLE) AS current_price_dbl, --casting current_price to double
    market_timestamp
  FROM {{ ref('silver_clean_stock_quotes') }} --referring to silver layer cleaned stock quotes data
  -- optionally filter invalid rows:
  WHERE TRY_CAST(current_price AS DOUBLE) IS NOT NULL -- filtering out rows where current_price cannot be cast to double
),

latest_day AS (
  -- if market_timestamp is epoch seconds (NUMBER/INT):
  SELECT CAST(TO_TIMESTAMP_LTZ(MAX(market_timestamp)) AS DATE) AS max_day -- getting the latest trading day from market_timestamp
  FROM source --referring to the prepared source data
),

latest_prices AS ( --calculating average price for the latest trading day
  SELECT
    symbol,
    AVG(current_price_dbl) AS avg_price
  FROM source
  JOIN latest_day ld
    ON CAST(TO_TIMESTAMP_LTZ(market_timestamp) AS DATE) = ld.max_day -- filtering for records from the latest trading day
  GROUP BY symbol
),

all_time_volatility AS ( --calculating all-time volatility and relative volatility for each stock symbol
  SELECT
    symbol,
    STDDEV_POP(current_price_dbl) AS volatility,             -- used standard deviation population for volatility. what is standard deviation in short terms? standard deviation is a measure of how much the stock price fluctuates around its average price. higher the standard deviation more the price fluctuates. lower the standard deviation more stable the price is. so volatility gives an idea of the risk associated with the stock. higher the volatility higher the risk.
    CASE -- calculating relative volatility
      WHEN AVG(current_price_dbl) = 0 THEN NULL -- avoiding division by zero
      ELSE STDDEV_POP(current_price_dbl) / NULLIF(AVG(current_price_dbl), 0) -- relative volatility formula
    END AS relative_volatility
  FROM source
  GROUP BY symbol
)

SELECT
  lp.symbol,
  lp.avg_price,
  v.volatility,
  v.relative_volatility
FROM latest_prices lp
JOIN all_time_volatility v ON lp.symbol = v.symbol
ORDER BY lp.symbol

/*in short and simple terms this view calculates key metrics for stock symbols such as average price for the latest trading day, all-time volatility, and relative volatility using cleaned stock quotes data from the silver layer. these metrics can help in analyzing stock performance and risk assessment. basically, average price gives an idea of the stock's value on the latest day, volatility indicates how much the stock price fluctuates over time, and relative volatility provides a normalized measure of volatility in relation to the stock's average price.
*/