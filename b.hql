SELECT
table1.col1 as alias1,
col1,
*
FROM
(SELECT table2.col2 as col1
  FROM
  (SELECT * FROM table3) table2
  JOIN side_table3 ON (table2.id = side_table3.id)) table1
  JOIN side_table1 ON (table1.id = side_table1.id)
  JOIN side_table2 side_alias ON (table1.id = side_table2.id)
