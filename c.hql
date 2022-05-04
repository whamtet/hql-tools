CREATE TABLE src (
  col1 STRING,
  col2 STRING
);

CREATE TABLE whamtet_in_out (
  col3 STRING
);

INSERT OVERWRITE TABLE whamtet_in_out
SELECT
  col1
FROM src;

SELECT * from whamtet_in_out;
