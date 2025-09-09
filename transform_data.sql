CREATE OR REPLACE TABLE
  `your-gcp-project-id.your-dataset-name.your-clean-table` AS
WITH
  ranked_prices AS (
    SELECT
      *,
      ROW_NUMBER() OVER(PARTITION BY currency_id ORDER BY ingestion_time DESC) AS rank_num
    FROM
      `your-gcp-project-id.your-dataset-name.your-raw-table`
    WHERE
      price_usd IS NOT NULL
  )
SELECT
  t1.currency_id,
  t1.price_usd,
  t1.market_cap_usd,
  t1.vol_24hr_usd,
  t1.change_24hr_usd,
  t1.ingestion_time,
  t1.ingestion_date,
  t1.ingestion_hour,
  t2.price_usd AS previous_price_usd,
  (t1.price_usd - t2.price_usd) / t2.price_usd AS price_change_percentage
FROM
  (
    SELECT
      currency_id,
      price_usd,
      market_cap_usd,
      vol_24hr_usd,
      change_24hr_usd,
      ingestion_time,
      CAST(ingestion_time AS DATE) AS ingestion_date,
      EXTRACT(HOUR FROM ingestion_time) AS ingestion_hour,
      rank_num
    FROM
      ranked_prices
  ) AS t1
LEFT JOIN
  ranked_prices AS t2
ON
  t1.currency_id = t2.currency_id
  AND t1.rank_num = t2.rank_num - 1
WHERE
  t1.rank_num = 1;