-- PROCEDURE: public.rm_4_lux_minus_table()

-- DROP PROCEDURE public.rm_4_lux_minus_table();

CREATE OR REPLACE PROCEDURE public.rm_4_lux_minus_table(
	)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE environnement VARCHAR;
DECLARE tstart TIMESTAMP; 
DECLARE tend TIMESTAMP;
DECLARE duration INTERVAL;

BEGIN

CALL public.log_message('Create Luxembourg one');

drop table if exists public.opportunity_li_fact_lux;

create table
	public.opportunity_li_fact_lux as 
select  
	*
FROM 
	public.opportunity_li_fact as oplif
where 
	country = 'Luxembourg';

	
drop table if exists public.opportunity_lux;
create table 
	public.opportunity_lux as 
SELECT 	
	"Id" as id,
	"Amount" as amout,
	"CloseDate" as close_date,
	"IsWon" as is_won,
	"CampaignId" as campaign_id,
	"s360a__Contact__c" as contact_id,
	"s360a__RegularGivingScheduleDate__c" as rg_date,
	"s360a__RegularGiving__c" as rg_id,
	"gpi__GiftCountryOfOwnership__c" as country,
	"RecordTypeId" as id_record_type
FROM 
	salesforce."Opportunity" as opp
WHERE
	"Id" in (select opportunity_id from public.opportunity_li_fact_lux);
	
	

CALL public.log_message('first');
tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('first step COMPLETE. Execution time: '||duration);
CALL public.log_message('CORRECTLY FINISH.');

END
$BODY$;
