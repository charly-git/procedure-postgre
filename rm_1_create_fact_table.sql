-- PROCEDURE: datamart.rm_1_create_fact_table()

-- DROP PROCEDURE datamart.rm_1_create_fact_table();

CREATE OR REPLACE PROCEDURE datamart.rm_1_create_fact_table(
	)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE environnement VARCHAR;
DECLARE tstart TIMESTAMP; 
DECLARE tend TIMESTAMP;
DECLARE duration INTERVAL;

BEGIN
environnement := 'dev';

CALL public.log_message('Clean');

CALL public.log_message('Create Datamart : START');
CALL public.log_message('Get opportunity > 2018 with campaign_id ');
tstart := clock_timestamp();

create table 
	datamart.tmp_opp as 
SELECT 
	opp."Id" 											as opportunity_id, 
	cast(campaign."s360a__GLSuffixValue__c" as integer)	as gl,
	opp."CampaignId" 					 as campaign_id, 
	opp."s360a__Contact__c" 			 as contact_id, 
	opp."s360a__RegularGiving__c" 		 as rg_id,
	opp."CloseDate"						 as date,
	opp."Paid_Date__c"					 as paid_date_lux,
	opp."gpi__GiftCountryOfOwnership__c" as country,
	CASE WHEN date_part('year', campaign."StartDate") = date_part('year', opp."CloseDate"	) THEN true ELSE false END is_current_year 
FROM 
	salesforce."Opportunity" AS opp
	LEFT JOIN "salesforce"."Campaign" AS campaign ON campaign."Id" = opp."CampaignId"
WHERE 
	opp."IsWon" is true
 	and date_part('year', opp."CloseDate") > 2018
	-- and "s360a__Contact__c" = '0033V00000H4BUwQAN'
	;

/* ***************************** LOG PART ********************************* */
tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('FINISH. Execution time: '||duration);
tstart := clock_timestamp();
CALL public.log_message('Add opportunity line item to previous select.');
/* ************************************************************************ */

create table 
	datamart.tmp_opli_fact as 
SELECT 
	opp.opportunity_id, 
	opp.gl, 
	opp.campaign_id, 
	opp.contact_id,
	opp.date,
	opp.paid_date_lux,
	opp.country,
	rg_id, opli."Id" as opli_id, 
	cast(opli."s360a__GLSuffixValue__c" as integer) as gl_opli,
	opli."s360a__RegularGivingLineItem__c" as rgli_id,
	opli."TotalPrice" as amount,
	opp.is_current_year
FROM 
	datamart.tmp_opp as opp 
	LEFT JOIN salesforce."OpportunityLineItem" as opli 
	ON opli."OpportunityId" = opp.opportunity_id;

/* ***************************** LOG PART ********************************* */
tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('FINISH. Execution time: '||duration);
tstart := clock_timestamp();
CALL public.log_message('Get RGLI with RG et GL.');
/* ************************************************************************ */

create table 
	datamart.tmp_rgli_fact as 
SELECT 
	cast(campaign."s360a__GLSuffixValue__c" as integer)	 as gl_rgli, 
	campaign."Name"						as name_campaign,
	rgli."Id" 							as rgli_id, 
	rgli."s360a__Campaign__c"			as campaign_id_rgli,
	rgli."s360a__RegularGiving__c"		as rg_id,
	rgli."s360a__ProcessFrom__c"		as process_from
FROM salesforce."s360a__RegularGivingLineItem__c" as rgli
	 LEFT JOIN salesforce."Campaign" as campaign 
	 ON rgli."s360a__Campaign__c" = campaign."Id";
	 
create table 
	datamart.tmp_rgli_fact_2 as 
SELECT 
	gl_rgli, 
	rgli_id, 
	campaign_id_rgli,
	name_campaign,
	process_from,
	rg."gpi__CountryOfOwnership__c" as country,
	rg."s360a__FirstPaymentDate__c"	as rgli_first_payment_date
FROM datamart.tmp_rgli_fact as rgli
	 LEFT JOIN salesforce."s360a__RegularGiving__c" as rg 
	 ON rgli.rg_id = rg."Id";

/* ***************************** LOG PART ********************************* */
tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('FINISH. Execution time: '||duration);
tstart := clock_timestamp();
CALL public.log_message('Reconciliation DU & RG.');
/* ************************************************************************ */

create table 
	datamart.tmp_li_fact as
SELECT 
	opli.opportunity_id, 
	CASE WHEN rgli.rgli_id is null THEN opli.gl ELSE rgli.gl_rgli END gl,
	CASE WHEN rgli.rgli_id is null THEN opli.campaign_id ELSE rgli.campaign_id_rgli END campaign_id, 
	rgli.name_campaign,
	rgli.process_from,
	
	opli.contact_id, 
	opli.rg_id, 
	opli.opli_id, 
	opli.date,
	opli.paid_date_lux,
	rgli.rgli_id,
	opli.amount,
	rgli.rgli_first_payment_date,
	CASE WHEN rgli.rgli_id is null THEN opli.country ELSE rgli.country END country,
	opli.is_current_year
	
FROM datamart.tmp_opli_fact as opli
	 left join datamart.tmp_rgli_fact_2 as rgli
	 on opli.rgli_id = rgli.rgli_id;
	 
	 
/* ***************************** LOG PART ********************************* */
tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('FINISH. Execution time: '||duration);
tstart := clock_timestamp();
CALL public.log_message('Get Campaign Member.');
/* ************************************************************************ */
	 
create table 
	datamart.tmp_opportunity_li_fact as
SELECT 
	opli_id, 
	opportunity_id, 
	campaign_id, 
	contact_id, 
	gl, 
	-- date, 
	CASE WHEN country = 'Luxembourg' THEN paid_date_lux ELSE date END date,
	rgli_id,
	country,
	cm."Id" as campaign_member_id,
	amount,
	op.rgli_first_payment_date,
	op.name_campaign,
	process_from,
	is_current_year
	
FROM datamart.tmp_li_fact as op
	 left join salesforce."CampaignMember" as cm
	 on op.campaign_id = cm."CampaignId" and op.contact_id = cm."ContactId" ;

/* ***************************** LOG PART ********************************* */
tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('FINISH. Execution time: '||duration);
tstart := clock_timestamp();
CALL public.log_message('Get previous opportunity.');
/* ************************************************************************ */

/*drop table datamart.get_previous_don;*/

create table 
	datamart.tmp_get_previous_don as
SELECT 
	opli_id,
	max(opp."CloseDate") as previous_opp_date
	
FROM datamart.tmp_opportunity_li_fact as opli
	 left join salesforce."Opportunity" as opp
	 on opli.contact_id = opp."s360a__Contact__c"
WHERE 
	opp."CloseDate" < opli.date
	and opp."StageName" = 'Closed Won'
	
GROUP BY
	opli_id;

create table
	datamart.tmp_opportunity_li_fact_2 as 
select  
	opli.opli_id, opportunity_id, campaign_id, contact_id, gl, date, rgli_id, country, campaign_member_id, 
	amount, previous_opp_date, name_campaign, process_from, rgli_first_payment_date, is_current_year
FROM 
	datamart.tmp_opportunity_li_fact as opli
	left join datamart.tmp_get_previous_don as prev 
	on opli.opli_id = prev.opli_id;

/* ***************************** LOG PART ********************************* */
tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('FINISH. Execution time: '||duration);
tstart := clock_timestamp();
CALL public.log_message('Get first donation date DU ou RG.');
/* ************************************************************************ */

create table 
	datamart.tmp_contact_first_donation_date as
SELECT 
	contact."Id" as id, 
	"s360a__FirstHardCreditDonationDate__c" as contact_first_donation_date
FROM 
	"salesforce"."Contact" as contact
	left join salesforce."Account" as account
	on contact."AccountId" = account."Id";

drop table if exists datamart.opportunity_li_fact;

create table
	datamart.opportunity_li_fact as 
select  
	opli.opli_id, opportunity_id, campaign_id, contact_id, gl, date, rgli_id, country, campaign_member_id, amount, previous_opp_date, 
	name_campaign, process_from, rgli_first_payment_date, contact_first_donation_date, is_current_year,

	CASE 
		WHEN date_part('year', rgli_first_payment_date) < 2021 OR date_part('year', contact_first_donation_date) < 2021 THEN 'Old' 
		ELSE 'New' END type_donateur
FROM 
	datamart.tmp_opportunity_li_fact_2 as opli
	left join datamart.tmp_contact_first_donation_date as contact 
	on opli.contact_id = contact.id;
	

UPDATE datamart.opportunity_li_fact op
SET name_campaign = camp."Name"	 
from salesforce."Campaign" camp
where op.campaign_id = camp."Id" 
and op.name_campaign is null;

	
if environnement = 'prod' then
	drop table datamart.opp_tmp;
	drop table datamart.opli_fact;
	drop table datamart.rgli_fact_tmp;
	drop table datamart.rgli_fact;
	drop table datamart.li_fact;
	drop table datamart.opportunity_li_fact_tmp;
	drop table datamart.opportunity_li_fact_tmp2;
	drop table datamart.get_previous_don;
end if;

GRANT SELECT ON datamart.opportunity_li_fact TO public;

tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('first step COMPLETE. Execution time: '||duration);
CALL public.log_message('CORRECTLY FINISH.');

CALL public.log_message('CLEAN.');
drop table if exists datamart.tmp_opp;
drop table if exists datamart.tmp_opli_fact;
drop table if exists datamart.tmp_rgli_fact;
drop table if exists datamart.tmp_rgli_fact_2;
drop table if exists datamart.tmp_li_fact;
drop table if exists datamart.tmp_opportunity_li_fact;
drop table if exists datamart.tmp_get_previous_don;
drop table if exists datamart.tmp_opportunity_li_fact_2;
drop table if exists datamart.tmp_contact_first_donation_date;
CALL public.log_message('CLEAN FINISH.');

GRANT SELECT ON datamart.opportunity_li_fact TO public;

END
$BODY$;

GRANT EXECUTE ON PROCEDURE datamart.rm_1_create_fact_table() TO public;

