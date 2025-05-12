SELECT 
   id,
   item_nbr,
   family,
   class,
   perishable
 FROM items
LIMIT {limit}
OFFSET {offset}