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
DECLARE previous_year INTEGER;
DECLARE previous_month INTEGER;

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

CALL public.rm_4_lux_minus_table();

/* ***************************** LOG PART ********************************* */
tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('FINISH. Execution time: '||duration);
tstart := clock_timestamp();
CALL public.log_message('Update vision contact month-1 and current month.');
/* ************************************************************************ */

current_month := date_part('month', CURRENT_DATE);
current_year := date_part('year', CURRENT_DATE);
previous_month := current_month-1;
previous_year := current_year;
if previous_month = 0 then
   previous_year := current_year-1;
   previous_month := 12;
end if;

CALL public.contact_historization(previous_month, previous_year);  
CALL public.contact_historization_aggregate(previous_month, previous_year); 

CALL public.contact_historization(current_month, current_year);  
CALL public.contact_historization_aggregate(current_month, current_year); 

/* ***************************** LOG PART ********************************* */
tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('Vision contact créée: '||duration);
tstart := clock_timestamp();
CALL public.log_message('Last Clean.');
/* ************************************************************************ */

CALL public._os_clean_tmp_table();

tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('Update Tableau : '||current_month||'/'||current_year);
CALL public.log_message('CORRECTLY FINISH.');

END
$BODY$;

GRANT EXECUTE ON PROCEDURE public.update_tableau() TO nsuch WITH GRANT OPTION;

GRANT EXECUTE ON PROCEDURE public.update_tableau() TO csadorge;

GRANT EXECUTE ON PROCEDURE public.update_tableau() TO PUBLIC;

