-- PROCEDURE: public.rm_3_get_dim_from_fact()

-- DROP PROCEDURE public.rm_3_get_dim_from_fact();

CREATE OR REPLACE PROCEDURE public.rm_3_get_dim_from_fact(
	)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE tstart TIMESTAMP;
DECLARE tend TIMESTAMP;
DECLARE duration INTERVAL;

BEGIN
CALL public.log_message('Create Datamart : START');

/* ***************************** LOG PART ********************************* */
tstart := clock_timestamp();
CALL public.log_message('get contact.');
/* ************************************************************************ */

create table 
	public.contact_tmp
as
SELECT 
	contact."Id" as id, 
	"s360a__FirstHardCreditDonationDate__c" as first_donation_date,
	"s360a__LastHardCreditDonationDate__c" as last_donation_date,
	"LastName" as last_name,
	"FirstName" as first_name,
	"MailingPostalCode" as mailing_postal_code,
	"Email" as email,
	"gpi__Lead_Sign_Up_Date__c" as lead_sign_up_date,
	"N1__c" as segmentation_n1,
	"N2__c" as segmentation_n2
FROM 
	"salesforce"."Contact" as contact
	left join salesforce."Account" as account
	on contact."AccountId" = account."Id"
WHERE
	contact."Id" in ( select contact_id from opportunity_li_fact);

drop table public.contact;
create table 
	public.contact as
SELECT 
	id, first_donation_date, last_donation_date, count("Id") as nb_rg, last_name, first_name, mailing_postal_code, email, lead_sign_up_date, segmentation_n1, segmentation_n2
FROM 
	contact_tmp AS contact
	left join salesforce."s360a__RegularGiving__c" as rg
	on contact.id = rg."s360a__Contact__c"
GROUP BY
	id, first_donation_date, last_donation_date, last_name, first_name, mailing_postal_code, email, lead_sign_up_date, segmentation_n1, segmentation_n2;

drop table public.contact_tmp;

/* ***************************** LOG PART ********************************* */
tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('FINISH. Execution time: '||duration);
tstart := clock_timestamp();
CALL public.log_message('get campaign_member.');
/* ************************************************************************ */
drop table if exists public.campaign_member;
create table 
	public.campaign_member 
as
SELECT 
	"Id" as id,
	"CampaignId" as campaign_id,
	"ContactId" as contact_id,
	"CreatedDate" as created_date,
	"LeadOrContactId" as lead_or_contact_id,
	"s360aie__Outcome__c" as outcome,
	"s360aie__Queue_Status__c" as queue_status,
	"s360aie__ResponseCode__c" as response_code 
	/* tfr call date : date d'appel 
		*/
FROM 
	"salesforce"."CampaignMember" 
WHERE
	"Id" in (select campaign_member_id from public.opportunity_li_fact where campaign_member_id is not null);

	
/*
drop table if exists public.campaign_member_contact;
create table 
	public.campaign_member_contact 
as
SELECT 
	"CampaignId" as campaign_id,
	"ContactId" as contact_id	
FROM 
	"salesforce"."CampaignMember" cm
WHERE
	"CampaignId" like '7013V000001Fowm%' or "CampaignId" like '7013V000001Foww%'
	
	"ContactId" in (select contact_id from public.opportunity_li_fact)
	and date_part('year', cm."CreatedDate") > 2019;
	
	
SELECT   COUNT(*) AS nbr_doublon, contact_id
FROM     public.campaign_member_contact
GROUP BY contact_id
HAVING   COUNT(*) > 1	
	
	
	*/
	
/* ***************************** LOG PART ********************************* */
tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('FINISH. Execution time: '||duration);
tstart := clock_timestamp();
CALL public.log_message('get opportunity.');
/* ************************************************************************ */
	
drop table public.opportunity;
create table 
	public.opportunity as 
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
	"Id" in (select opportunity_id from public.opportunity_li_fact);
	
	
	

drop table public.campaign;

create table 
	public.campaign as 
SELECT
	"Id" 						as id,
	"Name"						as name,
	"Type"						as type,
	"s360aie__TFRPartner__c"	as tfr_partner,
	"gpi__GP_Channel__c"		as channel,
	"gpi__Sub_Channel__c"		as sub_channel,
	"gpi__Media_Channel__c"		as media_channel,
	"gpi__Programme__c"			as programme
	
FROM 
	salesforce."Campaign"
WHERE
	"Id" in (select campaign_id from public.opportunity_li_fact);
	
tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('first step COMPLETE. Execution time: '||duration);

END
$BODY$;

GRANT EXECUTE ON PROCEDURE public.rm_3_get_dim_from_fact() TO nsuch WITH GRANT OPTION;

GRANT EXECUTE ON PROCEDURE public.rm_3_get_dim_from_fact() TO csadorge;

GRANT EXECUTE ON PROCEDURE public.rm_3_get_dim_from_fact() TO PUBLIC;
