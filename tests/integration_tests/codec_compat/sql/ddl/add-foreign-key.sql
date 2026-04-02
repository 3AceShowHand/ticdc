ALTER TABLE codec_compat.child_table
ADD CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES codec_compat.parent_table(id);
