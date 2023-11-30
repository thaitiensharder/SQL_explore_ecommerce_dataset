-- DAC.Query01
SELECT
  CONCAT(EXTRACT(YEAR FROM (PARSE_DATE('%Y%m%d', date))),0,
    EXTRACT(MONTH FROM (PARSE_DATE('%Y%m%d', date)))) AS month,
  SUM(totals.visits) AS total_visits,
  SUM(totals.pageviews) AS total_pageviews,
  SUM(totals.transactions) AS total_transactions
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` 
WHERE _table_suffix BETWEEN '0101' AND '0331'
GROUP BY month
ORDER BY month;

-- DAC.Query02
SELECT
  DISTINCT trafficSource.source,
  COUNT(totals.visits) AS total_visits,
  COUNT(totals.bounces) AS total_no_of_bounces,
  (COUNT(totals.bounces)/COUNT(totals.visits))*100 AS bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
WHERE _table_suffix BETWEEN '0701' AND '0731'
GROUP BY trafficSource.source
ORDER BY total_visits DESC;

-- DAC.Query03
WITH cte AS
  (SELECT
      CONCAT(EXTRACT(YEAR FROM (PARSE_DATE('%Y%m%d', date))),'0',
        EXTRACT(MONTH FROM (PARSE_DATE('%Y%m%d', date)))) AS time,
      trafficSource.source AS source,
      SUM(productRevenue)/1000000 AS revenue,
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
      UNNEST (hits) AS hits,
      UNNEST (hits.product) AS product
  WHERE _table_suffix BETWEEN '0601' AND '0630'
  GROUP BY time, source
  UNION ALL
  SELECT
    CONCAT(EXTRACT(YEAR FROM (PARSE_DATE('%Y%m%d', date))),
      EXTRACT(WEEK FROM (PARSE_DATE('%Y%m%d', date)))) AS time,
    trafficSource.source AS source,
    SUM(productRevenue)/1000000 AS revenue,
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
    UNNEST (hits) AS hits,
    UNNEST (hits.product) AS product
  WHERE _table_suffix BETWEEN '0601' AND '0630'
  GROUP BY time, source
  ORDER BY source, time)
SElECT 
  CASE WHEN time = '20170_' THEN 'Month'
    ELSE 'Week' END AS time_type,
  time, source, revenue
FROM cte;

-- DAC.Query04
WITH 
  cte1 AS
    (SELECT
      CONCAT(EXTRACT(YEAR FROM (PARSE_DATE('%Y%m%d', date))),'0',
            EXTRACT(MONTH FROM (PARSE_DATE('%Y%m%d', date)))) AS month,
      SUM(totals.pageviews)/COUNT(DISTINCT fullVisitorId) AS avg_pageviews_non_purchase
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
      UNNEST (hits) AS hits,
      UNNEST (hits.product) AS product
    WHERE _table_suffix BETWEEN '0601' AND '0731'
      AND totals.transactions IS NULL
      AND product.productRevenue IS NULL
    GROUP BY month),
  cte2 AS
    (SELECT
      CONCAT(EXTRACT(YEAR FROM (PARSE_DATE('%Y%m%d', date))),'0',
            EXTRACT(MONTH FROM (PARSE_DATE('%Y%m%d', date)))) AS month,
      SUM(totals.pageviews)/COUNT(DISTINCT fullVisitorId) AS avg_pageviews_purchase
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
      UNNEST (hits) AS hits,
      UNNEST (hits.product) AS product
    WHERE _table_suffix BETWEEN '0601' AND '0731'
      AND totals.transactions >= 1
      AND product.productRevenue IS NOT NULL
    GROUP BY month)
SELECT month, avg_pageviews_purchase, avg_pageviews_non_purchase
FROM cte1 INNER JOIN cte2
USING(month)
ORDER BY month;

-- DAC.Query05
SELECT 
  CONCAT(EXTRACT(YEAR FROM (PARSE_DATE('%Y%m%d', date))),'0',
            EXTRACT(MONTH FROM (PARSE_DATE('%Y%m%d', date)))) AS month,
  SUM(totals.transactions)/COUNT(DISTINCT fullVisitorId)
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
  UNNEST (hits) AS hits,
      UNNEST (hits.product) AS product
WHERE _table_suffix BETWEEN '0701' AND '0731'
  AND totals.transactions >= 1
  AND product.productRevenue IS NOT NULL
GROUP BY month;

-- DAC.Query06
SELECT
  CONCAT(EXTRACT(YEAR FROM (PARSE_DATE('%Y%m%d', date))),'0',
            EXTRACT(MONTH FROM (PARSE_DATE('%Y%m%d', date)))) AS month,
  ROUND(SUM(product.productRevenue)/(SUM(totals.visits)*1000000),3) AS avg_revenue_by_user_per_visit
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
  UNNEST (hits) AS hits,
  UNNEST (hits.product) AS product
WHERE _table_suffix BETWEEN '0701' AND '0731'
  AND totals.transactions >= 1
  AND product.productRevenue IS NOT NULL
GROUP BY month;

-- DAC.Query07
SELECT 
  DISTINCT v2ProductName AS other_purchased_products,
  SUM(productQuantity) AS quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
  UNNEST(hits) AS hits,
  UNNEST(hits.product) AS product
WHERE _table_suffix BETWEEN '0701' AND '0731'
  AND product.productRevenue IS NOT NULL
  AND v2ProductName NOT LIKE 'YouTube Men%Vintage Henley'
  AND fullVisitorID IN
    (SELECT 
      DISTINCT fullVisitorId
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
    UNNEST(hits) AS hits,
    UNNEST(hits.product) AS product
  WHERE _table_suffix BETWEEN '0701' AND '0731'
    AND product.productRevenue IS NOT NULL
    AND v2ProductName LIKE 'YouTube Men%Vintage Henley')
GROUP BY v2ProductName
ORDER BY quantity DESC;

-- DAC.Query08
WITH
  cte1 AS
    (SELECT
      CONCAT(EXTRACT(YEAR FROM (PARSE_DATE('%Y%m%d', date))),'0',
                EXTRACT(MONTH FROM (PARSE_DATE('%Y%m%d', date)))) AS month,
      COUNT(hits.eCommerceAction.action_type) AS num_product_view
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
      UNNEST(hits) AS hits
    WHERE _table_suffix BETWEEN '0101' AND '0331'
      AND hits.eCommerceAction.action_type = '2'
    GROUP BY month),
  cte2 AS
    (SELECT
      CONCAT(EXTRACT(YEAR FROM (PARSE_DATE('%Y%m%d', date))),'0',
                EXTRACT(MONTH FROM (PARSE_DATE('%Y%m%d', date)))) AS month,
      COUNT(hits.eCommerceAction.action_type) AS num_addtocart
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
      UNNEST(hits) AS hits
    WHERE _table_suffix BETWEEN '0101' AND '0331'
      AND hits.eCommerceAction.action_type = '3'
    GROUP BY month),
  cte3 AS
    (SELECT
      CONCAT(EXTRACT(YEAR FROM (PARSE_DATE('%Y%m%d', date))),'0',
                EXTRACT(MONTH FROM (PARSE_DATE('%Y%m%d', date)))) AS month,
      COUNT(hits.eCommerceAction.action_type) AS num_purchase
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
      UNNEST(hits) AS hits,
      UNNEST(hits.product) AS product
    WHERE _table_suffix BETWEEN '0101' AND '0331'
      AND hits.eCommerceAction.action_type = '6'
      AND product.productRevenue IS NOT NULL
    GROUP BY month)
SELECT month, num_product_view, num_addtocart, num_purchase,
  ROUND((num_addtocart/num_product_view * 100),2) AS add_to_cart_rate,
  ROUND((num_purchase/num_product_view * 100),2) AS purchase_rate
FROM cte1
  LEFT JOIN cte2
  USING(month) 
  LEFT JOIN cte3
  USING(month)
ORDER BY month;