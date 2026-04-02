CREATE TABLE codec_compat.dml_types_table (
  id INT PRIMARY KEY,
  c_decimal DECIMAL(10, 2),
  c_json JSON,
  c_date DATE,
  c_timestamp TIMESTAMP NULL,
  c_bool BOOLEAN,
  c_varchar VARCHAR(64)
);
