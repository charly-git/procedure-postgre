-- PROCEDURE: public.log_message(text)

-- DROP PROCEDURE public.log_message(text);

CREATE OR REPLACE PROCEDURE public.log_message(
	msg text)
LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
	RAISE NOTICE '% public %', clock_timestamp(), msg;
END
$BODY$;

GRANT EXECUTE ON PROCEDURE public.log_message(text) TO nsuch WITH GRANT OPTION;

GRANT EXECUTE ON PROCEDURE public.log_message(text) TO csadorge;

GRANT EXECUTE ON PROCEDURE public.log_message(text) TO PUBLIC;

