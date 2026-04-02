INSERT INTO codec_compat.dml_basic_table (id, c1, c2) VALUES (1, 10, 'alpha');
UPDATE codec_compat.dml_basic_table SET c1 = 11, c2 = 'beta' WHERE id = 1;
DELETE FROM codec_compat.dml_basic_table WHERE id = 1;
INSERT INTO codec_compat.dml_basic_table (id, c1, c2) VALUES (2, NULL, 'nullable');
