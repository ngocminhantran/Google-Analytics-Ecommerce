
-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL
SELECT 
    format_date('%Y%m', parse_date('%Y%m%d',date)) as month, 
    SUM(totals.visits) as visits, 
    SUM(totals.pageviews) as pageviews,
    SUM(totals.transactions) as transactions,
    SUM(totals.totalTransactionRevenue)/power(10,6) as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
WHERE _table_suffix between '0101' and '0331'
GROUP BY 1  
ORDER BY 1  


-- Query 02: Bounce rate per traffic source in July 2017
#standardSQL
SELECT
    trafficSource.source as source,
    sum(totals.visits) as total_visits,
    sum(totals.Bounces) as total_no_of_bounces,
    (sum(totals.Bounces)/sum(totals.visits))* 100 as bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
GROUP BY source
ORDER BY total_visits DESC



-- Query 3: Revenue by traffic source by week, by month in June 2017
With month_data as(
SELECT
  "Month" as time_type,
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  trafficSource.source AS source,
  SUM(totals.totalTransactionRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
_TABLE_SUFFIX BETWEEN '20170601' AND '20170631'
GROUP BY 1,2,3
ORDER BY revenue DESC
),

week_data as(
SELECT
  "Week" as time_type,
  format_date("%Y%W", parse_date("%Y%m%d", date)) as date,
  trafficSource.source AS source,
  SUM(totals.totalTransactionRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
_TABLE_SUFFIX BETWEEN '20170601' AND '20170631'
GROUP BY 1,2,3
ORDER BY revenue DESC
)

SELECT * FROM month_data
union all
SELECT * FROM week_data



--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL
With purchase as (
  SELECT 
      format_date('%Y%m', parse_date('%Y%m%d',date)) as month, 
      ROUND(SUM(totals.pageviews)/COUNT(distinct fullvisitorId),8) as avg_pageviews_purchase
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
  WHERE _table_suffix between '0601' and '0731' and totals.transactions >= 1
  GROUP BY month),
non_purchase as (
  SELECT format_date('%Y%m', parse_date('%Y%m%d',date)) as month, 
        ROUND(SUM(totals.pageviews)/COUNT(distinct fullvisitorId),8) as avg_pageviews_non_purchase
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
  WHERE _table_suffix between '0601' and '0731' and totals.transactions is null
  GROUP BY month
)

SELECT p.*
       ,avg_pageviews_non_purchase

FROM purchase p
LEFT JOIN non_purchase np
USING(month)
ORDER BY p.month 


-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL
SELECT 
    format_date('%Y%m', parse_date('%Y%m%d',date)) as month, 
    round(SUM(totals.transactions)/COUNT(distinct fullvisitorId),10) as avg_total_transactions_per_user
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
WHERE totals.transactions >= 1
GROUP BY month

-- Query 06: Average amount of money spent per session
#standardSQL

SELECT format_date('%Y%m', parse_date('%Y%m%d',date)) as month, 
        round(SUM(totals.totalTransactionRevenue)/COUNT(totals.visits),2) as avg_revenue_by_user_per_visit
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
WHERE totals.transactions >= 1
GROUP BY month

-- Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.
#standardSQL
WITH buyer_list as(
    SELECT
        distinct fullVisitorId
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
    , UNNEST(hits) AS hits
    , UNNEST(hits.product) as product
    WHERE product.v2ProductName = "YouTube Men's Vintage Henley"
    AND totals.transactions>=1
    AND product.productRevenue is not null
)

SELECT
  product.v2ProductName AS other_purchased_products,
  SUM(product.productQuantity) AS quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
JOIN buyer_list using(fullVisitorId)
WHERE product.v2ProductName != "YouTube Men's Vintage Henley"
 and product.productRevenue is not null
GROUP BY other_purchased_products
ORDER BY quantity DESC


--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL
With product_data as(
SELECT
    format_date('%Y%m', parse_date('%Y%m%d',date)) as month,
    count(CASE WHEN eCommerceAction.action_type = '2' THEN product.v2ProductName END) as num_product_view,
    count(CASE WHEN eCommerceAction.action_type = '3' THEN product.v2ProductName END) as num_add_to_cart,
    count(CASE WHEN eCommerceAction.action_type = '6' and product.productRevenue is not null THEN product.v2ProductName END) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
,UNNEST(hits) as hits
,UNNEST (hits.product) as product
WHERE _table_suffix between '20170101' and '20170331'
and eCommerceAction.action_type in ('2','3','6')   
GROUP BY month
)

SELECT 
    month, 
    num_product_view, 
    num_add_to_cart, 
    num_purchase, 
    round(num_add_to_cart/num_product_view*100,2) as add_to_cart_rate, 
    round(num_purchase/num_product_view*100,2) as purchase_rate
FROM product_data
ORDER BY month








