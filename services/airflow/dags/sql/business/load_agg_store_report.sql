INSERT OVERWRITE TABLE sales_business.store_report
SELECT
  sf.`date`,
  sf.store_nbr,
  city,
  state,
  dcoilwtico,
  unit_sales,
  transactions
from sales_business.sales_limit_fact sf
left join sales_business.stores_dim b on sf.store_nbr=b.store_nbr
left join sales_business.transactions_fact c on sf.`date`=c.`date`
left join sales_business.oil_fact d on sf.`date`=d.`date`
