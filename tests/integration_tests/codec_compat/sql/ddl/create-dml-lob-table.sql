CREATE TABLE codec_compat.dml_lob_table (
  id INT PRIMARY KEY,
  c_char_5 CHAR(5),
  c_varchar_32 VARCHAR(32),
  c_varchar_255 VARCHAR(255),
  c_binary_4 BINARY(4),
  c_varbinary_16 VARBINARY(16),
  c_tinytext TINYTEXT,
  c_text TEXT,
  c_mediumtext MEDIUMTEXT,
  c_longtext LONGTEXT,
  c_tinyblob TINYBLOB,
  c_blob BLOB,
  c_mediumblob MEDIUMBLOB,
  c_longblob LONGBLOB,
  c_utf8mb4 VARCHAR(64),
  c_gbk VARCHAR(64) CHARACTER SET gbk,
  c_gbk_text TEXT CHARACTER SET gbk
) CHARSET = utf8mb4;
