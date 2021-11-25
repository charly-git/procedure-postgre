-- PROCEDURE: public.contact_historization_aggregate(integer, integer)

-- DROP PROCEDURE public.contact_historization_aggregate(integer, integer);

CREATE OR REPLACE PROCEDURE public.contact_historization_aggregate(
	current_month integer,
	current_year integer)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE environnement VARCHAR;
DECLARE tstart TIMESTAMP;
DECLARE fulltimestart TIMESTAMP; 
DECLARE tend TIMESTAMP;
DECLARE duration INTERVAL;
DECLARE previous_year INTEGER;
DECLARE previous_month INTEGER;

BEGIN

previous_month := current_month-1;
previous_year := current_year;
if previous_month = 0 then
   previous_year := current_year-1;
   previous_month := 12;
end if;

environnement := 'dev';
CALL public.log_message('Clean');

CALL public.log_message('previous_month ' || previous_month);
CALL public.log_message('previous_year ' || previous_year);
CALL public.log_message('current_month ' || current_month);
CALL public.log_message('current_year ' || current_year);

CALL public.log_message('Create Datamart CONTACT : START');
CALL public.log_message('Get country et campagne acquisition');
CALL public.log_message('Get current month');
tstart := clock_timestamp();
fulltimestart := clock_timestamp();

CALL public.log_message('Get primary campaign from opportunity');
tstart := clock_timestamp();
fulltimestart := clock_timestamp();

drop table if exists public.tmp_first_opp;
create table 
	public.tmp_first_opp as
SELECT 
	"s360a__Contact__c" as contact_id,
	min("CloseDate") as first_date
	
FROM 
	salesforce."Opportunity"
WHERE 
	"IsWon" is true
GROUP BY
	"s360a__Contact__c";
	
drop table public.first_opp_w_contact;

create table
	public.first_opp_w_contact as
SELECT 
	opp."Id" as opp_id,
	"s360a__Contact__c" as contact_id,
	opp."s360a__RegularGiving__c" as rg_id,
	opp."CampaignId" as campaign_id
	
FROM public.tmp_first_opp as tfo
left join salesforce."Opportunity" as opp
	on tfo.contact_id = opp."s360a__Contact__c" and tfo.first_date = opp."CloseDate";
	

drop table if exists public.tmp_first_opp;

tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('Contact histo COMPLETE. Execution time: '||duration);

drop table if exists public.tmp_histo_contact;
create UNLOGGED table 
	public.tmp_histo_contact as 
SELECT  
	hc.*,
	contact."gpi__CountryOfOwnership__c" as country,
	contact."gpi__Primary_Campaign__c" as primary_campaign,
	case when contact."s360a__DeceasedDate__c" is null or contact."s360a__DeceasedDate__c" >= observation_date + interval '1 month' then true else false end alive,
	fowc.campaign_id as first_campaign_id
FROM 
	public.histo_contact as hc
	LEFT JOIN salesforce."Contact" as contact
	on hc.contact_id = contact."Id"
	LEFT JOIN public.first_opp_w_contact as fowc
	on hc.contact_id = fowc.contact_id
WHERE
	date_part('year', observation_date) = current_year
	and date_part('month', observation_date) = current_month;
	
/* ***************************** LOG PART ********************************* */
tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('FINISH. Execution time: '||duration);
tstart := clock_timestamp();
CALL public.log_message('Join histo_contact and current_contact');
/* ************************************************************************ */	
	

drop table if exists public.tmp_suivi_contact;
create UNLOGGED table 
	public.tmp_suivi_contact as 
SELECT  
	observation_date,
	country,
	primary_campaign,
	first_campaign_id,
	
	count(distinct contact_id) as nb_contact, 
	sum(nb_opportunity) as nb_opportunity, 
	sum(nb_du) as nb_du, 
	--sum(nb_pa) as nb_pa, 
	sum(nb_pa_actif) as nb_pa_actif,
	sum(arret_pa) as nb_arret_pa,
	sum(flag_upgrade) as nb_upgrade,
	sum(flag_downgrade) as nb_downgrade,
	--sum(nb_oppli_pa) as nb_oppli_pa, 
	sum(amount) as amount,
	nb_month_last_activity as last_activity,
	nb_month_last_du as last_du,
	nb_month_last_pa as last_pa,
	alive as alive
	/*CASE WHEN nb_month_last_activity  > 24 									THEN '24+' 
		 WHEN nb_month_last_activity <= 24 AND nb_month_last_activity > 12 	THEN '12-24'
		 WHEN nb_month_last_activity <= 12 									THEN '0-12' END last_activity*/
	
	
FROM 
	public.tmp_histo_contact as thc
GROUP BY 
	observation_date,
	country,
	primary_campaign,
	--last_activity,
	nb_month_last_activity,
	nb_month_last_du,
	nb_month_last_pa,
	alive,
	first_campaign_id;
	

/* ***************************** LOG PART ********************************* */
tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('FINISH. Execution time: '||duration);
tstart := clock_timestamp();
CALL public.log_message('Delete temp table');
/* ************************************************************************ */
DELETE FROM public.suivi_contact 
WHERE date_part('month', observation_date) = current_month
  AND date_part('year', observation_date) = current_year;
 
INSERT INTO public.suivi_contact (	observation_date, country, primary_campaign,
	nb_contact, nb_opportunity, nb_du, /*nb_pa,*/ nb_pa_actif, nb_arret_pa, nb_upgrade, nb_downgrade, /*nb_oppli_pa,*/ amount, last_activity, last_du, last_pa, alive, first_campaign_id)
SELECT observation_date, country, primary_campaign,
	nb_contact, nb_opportunity, nb_du, /*nb_pa,*/ nb_pa_actif, nb_arret_pa, nb_upgrade, nb_downgrade, /*nb_oppli_pa,*/ amount, last_activity, last_du, last_pa, alive, first_campaign_id
FROM public.tmp_suivi_contact;

drop table if exists public.tmp_histo_contact;

tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('Contact histo COMPLETE. Execution time: '||duration);
duration := tend - fulltimestart;
CALL public.log_message('COMPLETE. Full execution time: '||duration);
CALL public.log_message('CORRECTLY FINISH.');

END
$BODY$;

GRANT EXECUTE ON PROCEDURE public.contact_historization_aggregate(integer, integer) TO nsuch WITH GRANT OPTION;

GRANT EXECUTE ON PROCEDURE public.contact_historization_aggregate(integer, integer) TO csadorge;

GRANT EXECUTE ON PROCEDURE public.contact_historization_aggregate(integer, integer) TO PUBLIC;

