CREATE TABLE src (
  col1 STRING,
  col2 STRING
);

INSERT OVERWRITE TABLE whamtet_in_out
SELECT
  *
FROM src;

SELECT col2 FROM src;
