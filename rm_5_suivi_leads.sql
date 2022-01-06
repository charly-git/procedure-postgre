-- PROCEDURE: datamart.rm_5_suivi_leads()

-- DROP PROCEDURE datamart.rm_5_suivi_leads();

CREATE OR REPLACE PROCEDURE datamart.rm_5_suivi_leads(
	)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE environnement VARCHAR;
DECLARE tstart TIMESTAMP; 
DECLARE tend TIMESTAMP;
DECLARE duration INTERVAL;

BEGIN
environnement := 'dev';

CALL public.log_message('Create Suivi Lead : START');
CALL public.log_message('Get leads ');
tstart := clock_timestamp();

drop table if exists datamart.tmp_leads_leads;
create table 
	datamart.tmp_leads_leads as 
SELECT 	-- "Id", 
		"Email" as email,
		"gpi__CountryOfOwnership__c" as country,
		CASE WHEN "gpi__Lead_Sign_Up_Date__c" is null THEN "CreatedDate" ELSE "gpi__Lead_Sign_Up_Date__c" END date,
		CASE WHEN "Phone" is null THEN "MobilePhone" ELSE "Phone" END phone
	-- "IsConverted", "CreatedDate",
	-- "No_Email__c", "No_Mail__c", "No_Phone__c", "No_SMS__c", "s360a__CommunicateViaSMS__c", "s360a__ContactCodes__c", "gpi__CountryOfOwnership__c", "gpi__LeadCountryOfOwnership__c", 
FROM salesforce."Lead"
	where "IsConverted" is false;
	
drop table if exists datamart.tmp_leads_contact;
create table 
	datamart.tmp_leads_contact as 	
SELECT --"Id", 
		"Email" as email, 
		"gpi__CountryOfOwnership__c" as country,
		--"gpi__Lead_Sign_Up_Date__c", "AccountId", "HomePhone", 
		CASE 
			WHEN "gpi__Lead_Sign_Up_Date__c" is not null THEN "gpi__Lead_Sign_Up_Date__c" 
			WHEN "Signup_Date__c" is not null 			 THEN "Signup_Date__c"
			ELSE "CreatedDate"  END date,
		CASE WHEN "Phone" is null THEN "MobilePhone" ELSE "Phone" END phone
FROM salesforce."Contact"
where "AutomaticContactCodes__c" like '%Non Donor%'
 and ("Email" is not null or "Phone" is not null or "MobilePhone" is not null)
  and ("Email" not in (select email from datamart.tmp_leads_leads where email is not null) or "Email" is null);
	
/*	select count (*), date_part('year', "gpi__Lead_Sign_Up_Date__c")  as year
FROM salesforce."Contact" 
--where 
	group by date_part('year', "gpi__Lead_Sign_Up_Date__c") 
*/

drop table if exists datamart.tmp_leads;
create table 
	datamart.tmp_leads as 
	select * from datamart.tmp_leads_contact
	union 
	select * from datamart.tmp_leads_leads;
	
	

/* ***************************** LOG PART ********************************* */
tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('FINISH. Execution time: '||duration);
tstart := clock_timestamp();
CALL public.log_message('Keep the right part.');
/* ************************************************************************ */

drop table if exists datamart.suivi_leads;
create table 
	datamart.suivi_leads as 
SELECT 
	*
FROM 
	datamart.tmp_leads -- 2 166 791
WHERE
	date_part('year', date) > 2019 
	and country is not null;
	
-- 546697
-- 500518

tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('first step COMPLETE. Execution time: '||duration);
CALL public.log_message('CORRECTLY FINISH.');

CALL public.log_message('CLEAN.');
drop table if exists datamart.tmp_opp;
CALL public.log_message('CLEAN FINISH.');

END
$BODY$;
