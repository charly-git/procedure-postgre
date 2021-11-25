-- PROCEDURE: public.contact_historization(integer, integer)

-- DROP PROCEDURE public.contact_historization(integer, integer);

CREATE OR REPLACE PROCEDURE public.contact_historization(
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
CALL public.log_message('Alimenter la table historique des contacts');
CALL public.log_message('Get current month');
tstart := clock_timestamp();
fulltimestart := clock_timestamp();

-- comptage
drop table if exists public.tmp_current_month_contact_1;
create UNLOGGED table 
	public.tmp_current_month_contact_1 as 
SELECT  -- opli_id, campaign_id, rgli_id, campaign_member_id, previous_opp_date, rgli_first_payment_date, contact_first_donation_date, process_from
	contact_id , -- group
	TO_DATE('01' || lpad(date_part('month', date)::text, 2, '0') || date_part('year', date), 'DDMMYYYY') as observation_date, 
	opportunity_id,
	opli_id,
	amount,
	rgli_id,
	CASE WHEN rgli."s360a__RGLIType__c" like 'Upgrade%' 
		AND date_part('month',rgli."s360a__ProcessFrom__c") = current_month
		AND date_part('year',rgli."s360a__ProcessFrom__c") = current_year
	THEN 1 ELSE 0 END flag_upgrade,
	CASE WHEN rgli."s360a__RGLIType__c" like 'Downgrade%' 
		AND date_part('month',rgli."s360a__ProcessFrom__c") = current_month
		AND date_part('year',rgli."s360a__ProcessFrom__c") = current_year
	THEN 1 ELSE 0 END flag_downgrade
FROM 
	public.opportunity_li_fact -- tmp_oppli_full -- opportunity_li_fact -- public.tmp_opp_li_fact_2019 -- 
	AS opli
	LEFT JOIN salesforce."s360a__RegularGivingLineItem__c" as rgli
	on opli.rgli_id =  rgli."Id"
WHERE
	date_part('month', date) = current_month
	and date_part('year', date) = current_year;

-- regroupement
drop table if exists public.tmp_current_month_contact;
create table tmp_current_month_contact as
SELECT 	contact_id, observation_date, 
		count(distinct opportunity_id) as nb_opportunity,
		count(CASE WHEN rgli_id is null THEN opportunity_id END) as nb_du,
		count(CASE WHEN rgli_id is not null THEN ( opportunity_id) END) as nb_pa,
		count(CASE WHEN rgli_id is not null THEN ( opli_id) END) as nb_oppli_pa,
		sum(amount) as amount,
		sum(flag_upgrade) as flag_upgrade, sum(flag_downgrade) as flag_downgrade
FROM public.tmp_current_month_contact_1
GROUP BY contact_id, observation_date;
	

/* ***************************** LOG PART ********************************* */
tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('FINISH. Execution time: '||duration);
tstart := clock_timestamp();
CALL public.log_message('Join histo_contact and current_contact');
/* ************************************************************************ */

drop table if exists public.tmp_contact_to_update;
create UNLOGGED table 
	public.tmp_contact_to_update as 
SELECT  
	hc.contact_id,
	TO_DATE('01' || lpad(current_month::text, 2, '0') || current_year, 'DDMMYYYY') as observation_date,
	sum(case when cmc.nb_opportunity is not null then cmc.nb_opportunity else 0 end ) as nb_opportunity,
	sum(case when cmc.nb_du is not null then cmc.nb_du else 0 end) as nb_du,
	sum(case when cmc.nb_pa is not null then cmc.nb_pa else 0 end) as  nb_pa,
	sum(case when cmc.nb_oppli_pa is not null then cmc.nb_oppli_pa else 0 end) as  nb_oppli_pa,
	count(CASE WHEN rg."s360a__FirstPaymentDate__c" < TO_DATE('01' || lpad(current_month::text, 2, '0') || current_year, 'DDMMYYYY')
		AND ( rg."gpi__Cancelled_Date__c" is null OR rg."gpi__Cancelled_Date__c" > TO_DATE('01' || lpad(current_month::text, 2, '0') || current_year, 'DDMMYYYY') ) -- a désactiver en prod : réactive le PA
		-- AND	rg."s360a__RGStatus__c" IN ('Initial', 'Active', 'Last Attempt Failed', 'Held') -- uniquement pour la prod à commenter pour les reprises d'histos
	THEN rg."Id" END) as nb_pa_actif,
	CASE WHEN date_part('month',rg."gpi__Cancelled_Date__c") = current_month AND date_part('year',rg."gpi__Cancelled_Date__c") = current_year THEN 1 ELSE 0 END arret_pa,
	sum(cmc.amount) as amount,
	sum(cmc.flag_upgrade) as flag_upgrade,
	sum(cmc.flag_downgrade) as flag_downgrade,
	min(hc.nb_month_last_du) as nb_month_last_du, 
	min(hc.nb_month_last_pa) as nb_month_last_pa, 
	min(hc.nb_month_last_activity) as nb_month_last_activity
FROM 
	public.histo_contact as hc
	LEFT JOIN public.tmp_current_month_contact as cmc
		ON hc.contact_id = cmc.contact_id
	LEFT JOIN salesforce."s360a__RegularGiving__c" AS rg
		on hc.contact_id = rg."s360a__Contact__c"

WHERE
	date_part('year', hc.observation_date) = previous_year
	AND date_part('month', hc.observation_date) = previous_month
	
GROUP BY 
	hc.contact_id,
	rg."s360a__RGStatus__c",
	rg."gpi__Cancelled_Date__c";

/* ***************************** LOG PART ********************************* */
tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('FINISH. Execution time: '||duration);
tstart := clock_timestamp();
CALL public.log_message('update contact');
/* ************************************************************************ */

drop table if exists public.tmp_histo_update;

create UNLOGGED table 
	public.tmp_histo_update as 
SELECT  
	contact_id,
	observation_date,
	nb_opportunity,
	nb_du,
	nb_pa_actif,
	arret_pa,
	nb_pa,
	nb_oppli_pa,
	amount,
	flag_upgrade,
	flag_downgrade,
	CASE WHEN nb_du > 0 		 THEN 1 ELSE nb_month_last_du + 1 	 	END nb_month_last_du,
	CASE WHEN nb_pa > 0 		 THEN 1 ELSE nb_month_last_pa + 1 		END nb_month_last_pa,
	CASE WHEN nb_opportunity > 0 THEN 1 ELSE nb_month_last_activity + 1 	END nb_month_last_activity
FROM 
	public.tmp_contact_to_update;
	

/* ***************************** LOG PART ********************************* */
tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('FINISH. Execution time: '||duration);
tstart := clock_timestamp();
CALL public.log_message('new contact');
/* ************************************************************************ */

drop table if exists public.tmp_histo_insert_prep;

create UNLOGGED table 
	public.tmp_histo_insert_prep as 
SELECT
	tcmc.contact_id,
	tcmc.observation_date,
	sum(nb_opportunity) as nb_opportunity,
	sum(nb_du) as nb_du,
	CASE WHEN rg."s360a__FirstPaymentDate__c" < TO_DATE('01' || lpad(date_part('month', observation_date)::text, 2, '0') || date_part('year', observation_date), 'DDMMYYYY') 
		AND ( rg."gpi__Cancelled_Date__c" is null OR rg."gpi__Cancelled_Date__c" > TO_DATE('01' || lpad(date_part('month', observation_date)::text, 2, '0') || date_part('year', observation_date), 'DDMMYYYY') )
		-- AND	rg."s360a__RGStatus__c" IN ('Initial', 'Active', 'Last Attempt Failed', 'Held') -- uniquement pour la prod à commenter pour les reprises d'histos
	THEN count(rg."Id") ELSE 0 END nb_pa_actif,
	CASE WHEN rg."gpi__Cancelled_Date__c" = TO_DATE('01' || lpad(current_month::text, 2, '0') || current_year, 'DDMMYYYY') THEN 1 ELSE 0 END arret_pa,
	sum(nb_pa) as nb_pa,
	sum(nb_oppli_pa) as nb_oppli_pa,
	sum(amount) as amount,
	sum(flag_upgrade) as flag_upgrade,
	sum(flag_downgrade) as flag_downgrade,
	CASE WHEN nb_du > 0 		 THEN 1 	ELSE 99		END nb_month_last_du,
	CASE WHEN nb_pa > 0 		 THEN 1		ELSE 99 	END nb_month_last_pa,
	CASE WHEN nb_opportunity > 0 THEN 1 	ELSE 99 	END nb_month_last_activity
FROM
	public.tmp_current_month_contact as tcmc
	LEFT JOIN salesforce."s360a__RegularGiving__c" AS rg
		on tcmc.contact_id = rg."s360a__Contact__c"
WHERE
	contact_id not in (select distinct contact_id from public.tmp_histo_update where contact_id is not null)
GROUP BY 	
	tcmc.contact_id,
	tcmc.observation_date,
	nb_du,
	nb_pa,
	nb_opportunity,
	rg."s360a__RGStatus__c",
	rg."s360a__FirstPaymentDate__c",
	rg."gpi__Cancelled_Date__c";
--drop table if exists public.tmp_current_month_contact;
--drop table if exists public.tmp_current_month_contact;

drop table if exists public.tmp_histo_insert;

create UNLOGGED table 
	public.tmp_histo_insert as 
SELECT
	contact_id,
	observation_date,
	sum(nb_opportunity) as nb_opportunity,
	sum(nb_du) as nb_du,
	sum(nb_pa_actif) as nb_pa_actif,
	sum(arret_pa) as arret_pa,
	sum(nb_pa) as nb_pa,
	sum(nb_oppli_pa) as nb_oppli_pa,
	sum(amount) as amount,
	sum(flag_upgrade) as flag_upgrade,
	sum(flag_downgrade) as flag_downgrade,
	min(nb_month_last_du) as nb_month_last_du,
	min(nb_month_las-t_pa) as nb_month_last_pa,
	min(nb_month_last_activity) as nb_month_last_activity
FROM
	public.tmp_histo_insert_prep
GROUP BY
	contact_id,
	observation_date;

/* ***************************** LOG PART ********************************* */
tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('FINISH. Execution time: '||duration);
tstart := clock_timestamp();
CALL public.log_message('union current month insert and update data');
/* ************************************************************************ */

drop table if exists public.tmp_histo;

create UNLOGGED table 
	public.tmp_histo as 
SELECT * FROM  public.tmp_histo_insert
UNION ALL
SELECT * FROM  public.tmp_histo_update;

--drop table if exists public.tmp_contact_to_insert;

/* ***************************** LOG PART ********************************* */
tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('FINISH. Execution time: '||duration);
tstart := clock_timestamp();
CALL public.log_message('insert data in histo table');
/* ************************************************************************ */

/* au cas où il y a déjà des données pour ce mois ci, évite les doublons */
DELETE FROM public.histo_contact 
WHERE date_part('month', observation_date) = current_month
  AND date_part('year', observation_date) = current_year;
 
INSERT INTO public.histo_contact (	contact_id, observation_date,
									nb_opportunity, nb_du, nb_pa_actif, arret_pa, nb_pa, nb_oppli_pa, amount, flag_upgrade, flag_downgrade,
									nb_month_last_du, nb_month_last_pa, nb_month_last_activity)
SELECT contact_id, observation_date,
		nb_opportunity, nb_du, nb_pa_actif, arret_pa, nb_pa, nb_oppli_pa, amount, flag_upgrade, flag_downgrade,
		nb_month_last_du, nb_month_last_pa, nb_month_last_activity
FROM public.tmp_histo;

/*
select * from public.tmp_contact_to_update where contact_id is null

drop table if exists public.tmp_contact_to_update;
drop table if exists public.tmp_current_month_contact_1;
drop table if exists public.tmp_current_month_contact;
drop table if exists public.tmp_histo;
drop table if exists public.tmp_histo_insert;
drop table if exists public.tmp_histo_update;
*/
tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('Contact histo COMPLETE. Execution time: '||duration);
duration := tend - fulltimestart;
CALL public.log_message('COMPLETE. Full execution time: '||duration);
CALL public.log_message('CORRECTLY FINISH.');

END
$BODY$;

GRANT EXECUTE ON PROCEDURE public.contact_historization(integer, integer) TO nsuch WITH GRANT OPTION;

GRANT EXECUTE ON PROCEDURE public.contact_historization(integer, integer) TO csadorge;

GRANT EXECUTE ON PROCEDURE public.contact_historization(integer, integer) TO PUBLIC;

