INSERT OVERWRITE TABLE iceberg.sales_business.store_report
SELECT
  sf.`date`,
  sf.store_nbr,
  b.city,
  b.state,
  b.type as store_type,
  b.cluster as store_cluster,
  d.dcoilwtico as oil_price,
  SUM(sf.unit_sales) as total_sales,
  COUNT(DISTINCT sf.item_nbr) as unique_items_sold,
  SUM(CASE WHEN sf.onpromotion = 1 THEN sf.unit_sales ELSE 0 END) as promotional_sales,
  SUM(CASE WHEN sf.onpromotion = 0 THEN sf.unit_sales ELSE 0 END) as regular_sales,
  COUNT(DISTINCT CASE WHEN sf.onpromotion = 1 THEN sf.item_nbr ELSE NULL END) as items_on_promotion
FROM sales_business.sales_fact sf
LEFT JOIN sales_business.stores_dim b ON sf.store_nbr = b.store_nbr
LEFT JOIN sales_business.oil_fact d ON sf.`date` = d.`date`
GROUP BY 
  sf.`date`,
  sf.store_nbr,
  b.city,
  b.state,
  b.type,
  b.cluster,
  d.dcoilwtico
