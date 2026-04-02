INSERT INTO codec_compat.dml_types_table (
  id,
  c_decimal,
  c_json,
  c_date,
  c_timestamp,
  c_bool,
  c_varchar
) VALUES (
  1,
  123.45,
  JSON_OBJECT('k', 'v'),
  '2024-01-02',
  '2024-01-02 03:04:05',
  TRUE,
  'ascii'
);

UPDATE codec_compat.dml_types_table
SET c_decimal = 543.21,
    c_json = JSON_OBJECT('k', 'vv', 'n', 1),
    c_date = '2024-02-03',
    c_timestamp = '2024-02-03 04:05:06',
    c_bool = FALSE,
    c_varchar = 'changed'
WHERE id = 1;

DELETE FROM codec_compat.dml_types_table WHERE id = 1;
