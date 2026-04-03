-- signed and unsigned integer boundaries plus real and decimal variants
INSERT INTO codec_compat.dml_numeric_table (id) VALUES (1);

INSERT INTO codec_compat.dml_numeric_table (
  id,
  c_tinyint,
  c_smallint,
  c_mediumint,
  c_int,
  c_bigint,
  c_unsigned_tinyint,
  c_unsigned_smallint,
  c_unsigned_mediumint,
  c_unsigned_int,
  c_unsigned_bigint,
  c_float,
  c_unsigned_float,
  c_double,
  c_unsigned_double,
  c_decimal_10_2,
  c_decimal_38_10,
  c_decimal_unsigned,
  c_bool
) VALUES (
  2,
  1,
  2,
  3,
  4,
  5,
  1,
  2,
  3,
  4,
  5,
  1.5,
  2.5,
  3.5,
  4.5,
  123.45,
  1234567890.1234567890,
  9999.0001,
  TRUE
);

INSERT INTO codec_compat.dml_numeric_table (
  id,
  c_tinyint,
  c_smallint,
  c_mediumint,
  c_int,
  c_bigint,
  c_unsigned_tinyint,
  c_unsigned_smallint,
  c_unsigned_mediumint,
  c_unsigned_int,
  c_unsigned_bigint,
  c_float,
  c_unsigned_float,
  c_double,
  c_unsigned_double,
  c_decimal_10_2,
  c_decimal_38_10,
  c_decimal_unsigned,
  c_bool
) VALUES (
  3,
  -128,
  -32768,
  -8388608,
  -2147483648,
  -9223372036854775808,
  255,
  65535,
  16777215,
  4294967295,
  18446744073709551615,
  -2.5,
  2.5,
  -3.1415926,
  3.1415926,
  -99999999.99,
  1234567890123456789012345678.1234567890,
  9999999999999999.9999,
  FALSE
);

UPDATE codec_compat.dml_numeric_table
SET c_tinyint = 0,
    c_smallint = -1,
    c_mediumint = 0,
    c_int = 0,
    c_bigint = -1,
    c_unsigned_tinyint = 0,
    c_unsigned_smallint = 1,
    c_unsigned_mediumint = 2,
    c_unsigned_int = 3,
    c_unsigned_bigint = 4,
    c_float = 0,
    c_unsigned_float = 1.25,
    c_double = -0.5,
    c_unsigned_double = 5.25,
    c_decimal_10_2 = 0.01,
    c_decimal_38_10 = -1.0000000001,
    c_decimal_unsigned = 1.0000,
    c_bool = FALSE
WHERE id = 2;

UPDATE codec_compat.dml_numeric_table
SET id = 4,
    c_int = 42,
    c_bigint = 84,
    c_decimal_10_2 = 42.42
WHERE id = 3;

DELETE FROM codec_compat.dml_numeric_table WHERE id = 1;
