/* why create view inside sql. becuase we are connecting this project to power bi later. power bi can connect to sql views and tables directly. so creating views here makes it easier to connect to power bi. also creating views here allows us to reuse the logic in other models if needed. also creating views here allows us to version control the logic in dbt. we ar eusing direct query method instead of import method in power bi because direct query allows us to always get the latest data from snowflake without needing to refresh the data manually. this is important for real time stock data where prices change frequently. whenever we create direct query method it is best practice to create views in warehouse. so all the heavy lifting is done in snowflake and power bi just fetches the final data from snowflake. this improves performance and reduces load on power bi. power bi is just used for visualization and reporting. all the data transformations and calculations are done in snowflake via dbt models. if we create view in power bi it will convert the DAX  code to sql query then send it to warehouse. since data will be dynamic and changing frequently it may lead to performance issues. so best practice is to create views in snowflake via dbt and connect power bi to those views using direct query method.
what is kpi? kpi is key performance indicator. in stock market kpi can be current price, change amount, change percent etc. these are the key metrics that investors and traders look at to make decisions. in this model we are creating a view that shows the latest kpi for each stock symbol based on the cleaned data from silver layer.
*/

SELECT
symbol,
current_price,
change_amount,
change_percent,
--below is subquery to get latest market timestamp per symbol based on fetched_at timestamp in silver layer
FROM ( --subquery to get latest record per symbol based on fetched_at timestamp in silver layer
    SELECT *,
    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY fetched_at DESC) AS rn
    FROM {{ref('silver_clean_stock_quotes')}}
) t --t is an alias for the subquery
WHERE rn = 1 --filter to get only latest record per symbol 