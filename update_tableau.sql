-- PROCEDURE: public.update_tableau()

-- DROP PROCEDURE public.update_tableau();

CREATE OR REPLACE PROCEDURE public.update_tableau(
	)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE tstart TIMESTAMP; 
DECLARE tend TIMESTAMP;
DECLARE duration INTERVAL;
DECLARE current_year INTEGER;
DECLARE current_month INTEGER;

BEGIN

CALL public.log_message('Update Tableau : START');
tstart := clock_timestamp();

CALL public._os_clean_tmp_table();

/* ***************************** LOG PART ********************************* */
tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('Clean FINISH. Execution time: '||duration);
tstart := clock_timestamp();
CALL public.log_message('rm fact and dim tables.');
/* ************************************************************************ */

CALL public.rm_1_create_fact_table();
CALL public.rm_2_correctif_fact_table();
CALL public.rm_3_get_dim_from_fact();

/* ***************************** LOG PART ********************************* */
tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('Update Tableau FINISH. Execution time: '||duration);
tstart := clock_timestamp();
CALL public.log_message('Extract lux.');
/* ************************************************************************ */

CALL public.rm_4_lux_create_fact_table();

/* ***************************** LOG PART ********************************* */
tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('FINISH. Execution time: '||duration);
tstart := clock_timestamp();
CALL public.log_message('Create vision contact.');
/* ************************************************************************ */

current_month := date_part('month', CURRENT_DATE);
current_year := date_part('year', CURRENT_DATE);
CALL public.contact_historization(current_month, current_year);  
CALL public.contact_historization_aggregate(current_month, current_year); 

CALL public.log_message('Create vision contact. FINISH');

tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('Update Tableau : '||current_month||'/'||current_year);
CALL public.log_message('CORRECTLY FINISH.');

END
$BODY$;

GRANT EXECUTE ON PROCEDURE public.update_tableau() TO nsuch WITH GRANT OPTION;

GRANT EXECUTE ON PROCEDURE public.update_tableau() TO csadorge;

GRANT EXECUTE ON PROCEDURE public.update_tableau() TO PUBLIC;

