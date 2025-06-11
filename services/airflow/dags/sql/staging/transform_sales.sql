MERGE INTO {iceberg_db}.sales_fact T
USING (
    SELECT * FROM {iceberg_db_stg}.sales
) AS S
ON T.id = S.id
WHEN MATCHED THEN
UPDATE SET
    T.date = S.date,
    T.store_nbr = S.store_nbr,
    T.item_nbr = S.item_nbr,
    T.unit_sales = S.unit_sales,
    T.onpromotion = S.onpromotion
WHEN NOT MATCHED THEN
INSERT VALUES (
    S.id,
    S.date,
    S.store_nbr,
    S.item_nbr,
    S.unit_sales,
    S.onpromotion
)