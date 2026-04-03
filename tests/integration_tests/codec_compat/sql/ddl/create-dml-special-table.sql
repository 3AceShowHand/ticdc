CREATE TABLE codec_compat.dml_special_table (
  id INT PRIMARY KEY,
  c_enum ENUM('small', 'medium', 'large'),
  c_set SET('red', 'green', 'blue'),
  c_bit_1 BIT(1),
  c_bit_8 BIT(8),
  c_bit_64 BIT(64),
  c_json JSON,
  c_vector VECTOR(5)
);
