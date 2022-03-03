-- PROCEDURE: datamart.rm_2_correctif_fact_table()

-- DROP PROCEDURE datamart.rm_2_correctif_fact_table();

CREATE OR REPLACE PROCEDURE datamart.rm_2_correctif_fact_table(
	)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE tstart TIMESTAMP; 
DECLARE tend TIMESTAMP;
DECLARE duration INTERVAL;

BEGIN

CALL public.log_message('Update FACT TABLE : START');
CALL public.log_message('update GL');
tstart := clock_timestamp();

UPDATE  datamart.opportunity_li_fact SET gl=14000
	WHERE gl<>14000
	 AND gl<>14200
	 AND gl<>14201
	 AND date_part('year', rgli_first_payment_date) < 2021;
	 
UPDATE datamart.opportunity_li_fact SET gl=14000
	WHERE gl in (14200, 14201)
	  AND process_from < '2020-09-01'::date 
	  AND rgli_id is not null;
	  
UPDATE datamart.opportunity_li_fact SET gl=14201
	WHERE gl =14200 
	  AND process_from >= '2021-01-01'::date 
	  AND rgli_id is not null;
	
	
UPDATE datamart.opportunity_li_fact SET gl='14000' 
	WHERE gl<>'14000' and gl<>'14200' and gl<>'14201' 
	  AND process_from<'2021-01-01'::date ;
	  
UPDATE datamart.opportunity_li_fact SET gl='14000' 
	WHERE gl is null and process_from < '2021-01-01'::date ;
	
	
/* gestion des années précédentes */
	
UPDATE datamart.opportunity_li_fact SET gl='12100' 
	WHERE (name_campaign like 'FR DD%'
	  AND name_campaign not like'FR DD 2021%')
	  AND date_part('year', process_from) = 2021;
	  
UPDATE datamart.opportunity_li_fact SET gl='12700' 
	WHERE (name_campaign like 'FR WEB%' 				 
	  AND name_campaign not like'FR WEB 2021%')
	  AND date_part('year', process_from) = 2021;
	  
UPDATE datamart.opportunity_li_fact SET gl='12800' 
	WHERE (name_campaign like 'FR TMK%Lead Conversion%' 
	  AND name_campaign not like'FR TMK 2021 Lead Conversion%')
	  AND date_part('year', process_from) = 2021;
		   
UPDATE datamart.opportunity_li_fact SET gl='12800' 
	WHERE (name_campaign like 'FR REAC%Cycle%' 		 
	  AND name_campaign not like'FR REAC 2021 Cycle%') 		 
	  AND date_part('year', process_from) = 2021;

tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('first step COMPLETE. Execution time: '||duration);
CALL public.log_message('UPDATE FINISHED.');

END
$BODY$;

GRANT EXECUTE ON PROCEDURE datamart.rm_2_correctif_fact_table() TO csadorge;

GRANT EXECUTE ON PROCEDURE datamart.rm_2_correctif_fact_table() TO PUBLIC;

