CREATE TABLE my_tab (
  shop_name STRING,
  browsing_itemid ARRAY < STRING >
) PARTITIONED BY (service_type STRING)
