/* in this file we are transforming raw stock quotes data into structured format. basic data cleaning and type casting is done here. the data fetched from minio to the bronze_stock_quotes_raw table is in json format and all columns are string type. we are converting them to proper data types here. we dont need to do any complex transformations at this stage as this is just the bronze layer. more complex transformations will be done in silver and gold layers. we dont need to perform star schema because this is the stock data and it belongs to a single entity. star schema is more suitable for multi-entity data like sales data with customers, products, time dimensions etc.
note that the data is already loaded into the bronze_stock_quotes_raw table in snowflake via the airflow dag defined in infra/dags/minio_to_snowflake.py. do not confuse the bronze_stock_quotes_raw table with this model. this model is bronze_stg_stock_quotes which is a transformed version of the bronze_stock_quotes_raw table.
*/
SELECT 
v:c::float AS current_price,
v:d::float AS change_amount,
v:dp::float AS change_percent,
v:h::float AS day_high,
v:l::float AS day_low,
v:o::float AS day_open,
v:pc::float AS prev_close,
v:t::timestamp AS market_timestamp,
v:symbol::string AS symbol,
v:fetched_at::timestamp AS fetched_at
FROM {{source('raw', 'bronze_stock_quotes_raw')}}