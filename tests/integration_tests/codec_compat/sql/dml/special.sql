-- enum, set, bit, json, and vector coverage
INSERT INTO codec_compat.dml_special_table (
  id,
  c_enum,
  c_set,
  c_bit_1,
  c_bit_8,
  c_bit_64,
  c_json,
  c_vector
) VALUES (
  1,
  'small',
  'red,blue',
  b'1',
  b'10101010',
  x'0000000000000001',
  JSON_OBJECT('key', 'value', 'arr', JSON_ARRAY(1, 2, 3), 'nested', JSON_OBJECT('flag', TRUE)),
  '[1,2,3,4,5]'
);

INSERT INTO codec_compat.dml_special_table (
  id,
  c_enum,
  c_set,
  c_bit_1,
  c_bit_8,
  c_bit_64,
  c_json,
  c_vector
) VALUES (
  2,
  'medium',
  '',
  b'0',
  b'00000000',
  x'FFFFFFFFFFFFFFFF',
  CAST('null' AS JSON),
  '[0,-0.1,-2,2,0.1]'
);

UPDATE codec_compat.dml_special_table
SET c_enum = 'large',
    c_set = 'green',
    c_bit_1 = b'0',
    c_bit_8 = b'11110000',
    c_bit_64 = x'00000000000000FF',
    c_json = JSON_ARRAY('text', 1, TRUE, NULL, JSON_OBJECT('nested', 'vv')),
    c_vector = '[5,4,3,2,1]'
WHERE id = 1;

UPDATE codec_compat.dml_special_table
SET id = 3,
    c_enum = 'small',
    c_set = 'green,blue',
    c_bit_1 = b'1',
    c_bit_8 = b'00001111',
    c_json = JSON_OBJECT(
      'emptyObj', JSON_OBJECT(),
      'emptyArr', JSON_ARRAY(),
      'text', CONCAT('line', CHAR(10), 'value')
    ),
    c_vector = '[0.1,0.2,0.3,0.4,0.5]'
WHERE id = 2;

DELETE FROM codec_compat.dml_special_table WHERE id = 3;
