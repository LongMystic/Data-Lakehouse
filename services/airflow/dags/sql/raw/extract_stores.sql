SELECT
    id,
    store_nbr,
    city,
    state,
    type,
    cluster
FROM stores
LIMIT {limit}
OFFSET {offset}