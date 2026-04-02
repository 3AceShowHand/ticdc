CREATE VIEW codec_compat.v_view_base AS
SELECT id, c1
FROM codec_compat.view_base
WHERE c1 > 0;
