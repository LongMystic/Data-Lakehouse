SELECT
    id,
    store_nbr,
    date,
    transactions
FROM transactions
LIMIT {limit}
OFFSET {offset}