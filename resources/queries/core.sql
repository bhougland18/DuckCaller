-- name: simple_update
-- docs: A parameterized sql update statement where table, field, and value are required.
UPDATE :table
SET :field = :value

-- name: get_all_buyers
Select * From Buyer