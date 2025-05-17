INSERT OVERWRITE TABLE iceberg.sales_business.product_report
SELECT
  sf.`date`,
  sf.item_nbr,
  i.family as product_family,
  i.class as product_class,
  i.perishable,
  SUM(sf.unit_sales) as total_sales,
  COUNT(DISTINCT sf.store_nbr) as stores_sold_in,
  SUM(CASE WHEN sf.onpromotion = 1 THEN sf.unit_sales ELSE 0 END) as promotional_sales,
  SUM(CASE WHEN sf.onpromotion = 0 THEN sf.unit_sales ELSE 0 END) as regular_sales,
  COUNT(DISTINCT CASE WHEN sf.onpromotion = 1 THEN sf.store_nbr ELSE NULL END) as stores_with_promotion
FROM sales_business.sales_fact sf
LEFT JOIN sales_business.items_dim i ON sf.item_nbr = i.item_nbr
GROUP BY 
  sf.`date`,
  sf.item_nbr,
  i.family,
  i.class,
  i.perishable 