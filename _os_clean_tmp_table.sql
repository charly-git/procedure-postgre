-- PROCEDURE: public._os_clean_tmp_table()

-- DROP PROCEDURE public._os_clean_tmp_table();

CREATE OR REPLACE PROCEDURE public._os_clean_tmp_table(
	)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE tstart TIMESTAMP;
DECLARE fulltimestart TIMESTAMP; 
DECLARE tend TIMESTAMP;
DECLARE duration INTERVAL;
DECLARE row record;
BEGIN

CALL public.log_message('Clean all table begin by tmp_');
tstart := clock_timestamp();
fulltimestart := clock_timestamp();

FOR row IN 
 	SELECT
		table_schema,
		table_name
	FROM
		information_schema.tables
	WHERE 	table_type = 'BASE TABLE'
	AND 	table_schema = 'public'
	AND 	(table_name ILIKE ('tmp_%') or table_name ILIKE ('test%'))
	AND 	table_name <> 'tmp_tDBOutputBulkExec_2_tPBE_yXmBlf1'
LOOP
	EXECUTE 'DROP TABLE ' || quote_ident(row.table_schema) || '.' || quote_ident(row.table_name) || ' CASCADE ';
	RAISE INFO 'Dropped table: %', quote_ident(row.table_schema) || '.' || quote_ident(row.table_name);
END LOOP;

tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('Contact histo COMPLETE. Execution time: '||duration);
duration := tend - fulltimestart;
CALL public.log_message('COMPLETE. Full execution time: '||duration);
CALL public.log_message('CORRECTLY FINISH.');

END
$BODY$;
