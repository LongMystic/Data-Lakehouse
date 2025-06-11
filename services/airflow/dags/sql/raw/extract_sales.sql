SELECT
    id,
    date,
    store_nbr,
    item_nbr,
    unit_sales,
    onpromotion
FROM test.sales
WHERE date >= {from_date} AND date <= {to_date}