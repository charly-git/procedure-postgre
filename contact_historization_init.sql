-- PROCEDURE: datamart.contact_historization_init()

-- DROP PROCEDURE datamart.contact_historization_init();

CREATE OR REPLACE PROCEDURE datamart.contact_historization_init(
	)
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

CALL public.log_message('INIT');
CALL public.log_message('Alimenter la table historique des contacts');
CALL public.log_message('Get current month');
tstart := clock_timestamp();
fulltimestart := clock_timestamp();

drop table if exists datamart.histo_contact;
drop table if exists datamart.tmp_histo_contact;
drop table if exists datamart.tmp_histo_contact_2;
create table 
	datamart.tmp_histo_contact as 
SELECT  
	contact."Id" as contact_id , -- group
	TO_DATE('01122015', 'DDMMYYYY') as observation_date, 
	0 as nb_opportunity, -- group
	0 as nb_du, --group
	0 as nb_pa,
	0 as nb_oppli_pa,
	0 as amount,
	999 as nb_month_last_du, 
	999 as nb_month_last_pa, 
	999 as nb_month_last_activity
FROM 
	"salesforce"."Contact" as contact
	left join salesforce."Account" as account
	on contact."AccountId" = account."Id"
WHERE
	date_part('year', "s360a__FirstHardCreditDonationDate__c" ) < 2016
	AND 
	date_part('year', "s360a__LastHardCreditDonationDate__c" ) >= 2014
GROUP BY
	contact."Id";

create table 
	datamart.tmp_histo_contact_2 as 
SELECT  
	contact.contact_id,
	contact.observation_date, 
	contact.nb_opportunity,
	contact.nb_du,
	CASE WHEN rg."s360a__FirstPaymentDate__c" < TO_DATE('01122015', 'DDMMYYYY')
		AND  ( rg."gpi__Cancelled_Date__c" is null OR  rg."gpi__Cancelled_Date__c" > TO_DATE('01122015', 'DDMMYYYY') )
	THEN count(rg."Id") ELSE 0 END nb_pa_actif,
	0 as arret_pa,
	contact.nb_pa,
	contact.nb_oppli_pa,
	contact.amount,
	contact.nb_month_last_du, 
	contact.nb_month_last_pa, 
	contact.nb_month_last_activity
FROM 
	datamart.tmp_histo_contact AS contact
	left join salesforce."s360a__RegularGiving__c" AS rg
	on contact.contact_id = rg."s360a__Contact__c"
GROUP BY
	contact.contact_id,
	contact.observation_date, 
	contact.nb_opportunity,
	contact.nb_du,
	contact.nb_pa,
	contact.nb_oppli_pa,
	contact.amount,
	contact.nb_month_last_du, 
	contact.nb_month_last_pa, 
	contact.nb_month_last_activity,
	rg."s360a__FirstPaymentDate__c",
	rg."gpi__Cancelled_Date__c",
	rg."s360a__RGStatus__c";
	

create table 
	datamart.histo_contact as 
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
	0 as flag_upgrade, 
	0 as flag_downgrade,
	min(nb_month_last_du) as nb_month_last_du, 
	min(nb_month_last_pa) as nb_month_last_pa, 
	min(nb_month_last_activity) as nb_month_last_activity
FROM 
	datamart.tmp_histo_contact_2
GROUP BY
	contact_id,
	observation_date;
	
	
drop table if exists datamart.tmp_histo_contact;
drop table if exists datamart.tmp_histo_contact_2;
	
/* ***************************** LOG PART ********************************* */
tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('RESET FINISH. Execution time: '||duration);
tstart := clock_timestamp();
CALL public.log_message('charge data');
/* ************************************************************************ */

call datamart.contact_historization(1, 2016);  
call datamart.contact_historization(2, 2016);  
call datamart.contact_historization(3, 2016);   
call datamart.contact_historization(4, 2016);
call datamart.contact_historization(5, 2016);
call datamart.contact_historization(6, 2016);
call datamart.contact_historization(7, 2016);
call datamart.contact_historization(8, 2016);
call datamart.contact_historization(9, 2016);
call datamart.contact_historization(10, 2016);
call datamart.contact_historization(11, 2016);
call datamart.contact_historization(12, 2016);

/* ***************************** LOG PART ********************************* */
tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('2016 FINISH. Execution time: '||duration);
tstart := clock_timestamp();
CALL public.log_message('2017 : ');
/* ************************************************************************ */

call datamart.contact_historization(1, 2017);  
call datamart.contact_historization(2, 2017);  
call datamart.contact_historization(3, 2017);   
call datamart.contact_historization(4, 2017);
call datamart.contact_historization(5, 2017);
call datamart.contact_historization(6, 2017);
call datamart.contact_historization(7, 2017);
call datamart.contact_historization(8, 2017);
call datamart.contact_historization(9, 2017);
call datamart.contact_historization(10, 2017);
call datamart.contact_historization(11, 2017);
call datamart.contact_historization(12, 2017);

/* ***************************** LOG PART ********************************* */
tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('2017 FINISH. Execution time: '||duration);
tstart := clock_timestamp();
CALL public.log_message('2018 : ');
/* ************************************************************************ */

call datamart.contact_historization(1, 2018);  
call datamart.contact_historization(2, 2018);  
call datamart.contact_historization(3, 2018);   
call datamart.contact_historization(4, 2018);
call datamart.contact_historization(5, 2018);
call datamart.contact_historization(6, 2018);
call datamart.contact_historization(7, 2018);
call datamart.contact_historization(8, 2018);
call datamart.contact_historization(9, 2018);
call datamart.contact_historization(10, 2018);
call datamart.contact_historization(11, 2018);
call datamart.contact_historization(12, 2018);

/* ***************************** LOG PART ********************************* */
tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('2018 FINISH. Execution time: '||duration);
tstart := clock_timestamp();
CALL public.log_message('2019 : ');
/* ************************************************************************ */

call datamart.contact_historization(1, 2019);  
call datamart.contact_historization(2, 2019);  
call datamart.contact_historization(3, 2019);   
call datamart.contact_historization(4, 2019);
call datamart.contact_historization(5, 2019);
call datamart.contact_historization(6, 2019);
call datamart.contact_historization(7, 2019);
call datamart.contact_historization(8, 2019);
call datamart.contact_historization(9, 2019);
call datamart.contact_historization(10, 2019);
call datamart.contact_historization(11, 2019);
call datamart.contact_historization(12, 2019);

/* ***************************** LOG PART ********************************* */
tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('2019 FINISH. Execution time: '||duration);
tstart := clock_timestamp();
CALL public.log_message('2020 : ');
/* ************************************************************************ */

call datamart.contact_historization(1, 2020);  
call datamart.contact_historization(2, 2020);  
call datamart.contact_historization(3, 2020);   
call datamart.contact_historization(4, 2020);
call datamart.contact_historization(5, 2020);
call datamart.contact_historization(6, 2020);
call datamart.contact_historization(7, 2020);
call datamart.contact_historization(8, 2020);
call datamart.contact_historization(9, 2020);
call datamart.contact_historization(10, 2020);
call datamart.contact_historization(11, 2020);
call datamart.contact_historization(12, 2020);

/* ***************************** LOG PART ********************************* */
tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('2020 FINISH. Execution time: '||duration);
tstart := clock_timestamp();
CALL public.log_message('2021 : ');
/* ************************************************************************ */

call datamart.contact_historization(1, 2021);  
call datamart.contact_historization(2, 2021);  
call datamart.contact_historization(3, 2021);   
call datamart.contact_historization(4, 2021);
call datamart.contact_historization(5, 2021);
call datamart.contact_historization(6, 2021);
call datamart.contact_historization(7, 2021);
call datamart.contact_historization(8, 2021);
call datamart.contact_historization(9, 2021);
call datamart.contact_historization(10, 2021);
call datamart.contact_historization(11, 2021);
call datamart.contact_historization(12, 2021);

commit;

/* ***************************** LOG PART ********************************* */
tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('Init agréga. Execution time: '||duration);
tstart := clock_timestamp();
/* ************************************************************************ */

DROP TABLE if exists datamart.suivi_contact;

CREATE TABLE IF NOT EXISTS datamart.suivi_contact
(
    observation_date date,
    country text COLLATE pg_catalog."default",
    primary_campaign character varying(18) COLLATE pg_catalog."default",
    nb_contact bigint,
    nb_opportunity numeric,
    nb_du numeric,
    --nb_pa numeric,
    nb_pa_actif numeric,
	nb_arret_pa numeric,
    nb_upgrade bigint,
    nb_downgrade bigint,
    --nb_oppli_pa numeric,
    amount numeric,
    last_activity integer,
    last_du integer,
    last_pa integer,
	alive boolean,
	first_campaign_id character varying(18) COLLATE pg_catalog."default"
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

--ALTER TABLE datamart.suivi_contact
--    OWNER to sfuser;
	
	

call datamart.contact_historization_aggregate(1, 2016);  
call datamart.contact_historization_aggregate(2, 2016);  
call datamart.contact_historization_aggregate(3, 2016);   
call datamart.contact_historization_aggregate(4, 2016);
call datamart.contact_historization_aggregate(5, 2016);
call datamart.contact_historization_aggregate(6, 2016);
call datamart.contact_historization_aggregate(7, 2016);
call datamart.contact_historization_aggregate(8, 2016);
call datamart.contact_historization_aggregate(9, 2016);
call datamart.contact_historization_aggregate(10, 2016);
call datamart.contact_historization_aggregate(11, 2016);
call datamart.contact_historization_aggregate(12, 2016);

/* ***************************** LOG PART ********************************* */
tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('2016 FINISH. Execution time: '||duration);
tstart := clock_timestamp();
CALL public.log_message('2017 : ');
/* ************************************************************************ */

call datamart.contact_historization_aggregate(1, 2017);  
call datamart.contact_historization_aggregate(2, 2017);  
call datamart.contact_historization_aggregate(3, 2017);   
call datamart.contact_historization_aggregate(4, 2017);
call datamart.contact_historization_aggregate(5, 2017);
call datamart.contact_historization_aggregate(6, 2017);
call datamart.contact_historization_aggregate(7, 2017);
call datamart.contact_historization_aggregate(8, 2017);
call datamart.contact_historization_aggregate(9, 2017);
call datamart.contact_historization_aggregate(10, 2017);
call datamart.contact_historization_aggregate(11, 2017);
call datamart.contact_historization_aggregate(12, 2017);

/* ***************************** LOG PART ********************************* */
tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('2017 FINISH. Execution time: '||duration);
tstart := clock_timestamp();
CALL public.log_message('2018 : ');
/* ************************************************************************ */

call datamart.contact_historization_aggregate(1, 2018);  
call datamart.contact_historization_aggregate(2, 2018);  
call datamart.contact_historization_aggregate(3, 2018);   
call datamart.contact_historization_aggregate(4, 2018);
call datamart.contact_historization_aggregate(5, 2018);
call datamart.contact_historization_aggregate(6, 2018);
call datamart.contact_historization_aggregate(7, 2018);
call datamart.contact_historization_aggregate(8, 2018);
call datamart.contact_historization_aggregate(9, 2018);
call datamart.contact_historization_aggregate(10, 2018);
call datamart.contact_historization_aggregate(11, 2018);
call datamart.contact_historization_aggregate(12, 2018);

/* ***************************** LOG PART ********************************* */
tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('2018 FINISH. Execution time: '||duration);
tstart := clock_timestamp();
CALL public.log_message('2019 : ');
/* ************************************************************************ */

call datamart.contact_historization_aggregate(1, 2019);  
call datamart.contact_historization_aggregate(2, 2019);  
call datamart.contact_historization_aggregate(3, 2019);   
call datamart.contact_historization_aggregate(4, 2019);
call datamart.contact_historization_aggregate(5, 2019);
call datamart.contact_historization_aggregate(6, 2019);
call datamart.contact_historization_aggregate(7, 2019);
call datamart.contact_historization_aggregate(8, 2019);
call datamart.contact_historization_aggregate(9, 2019);
call datamart.contact_historization_aggregate(10, 2019);
call datamart.contact_historization_aggregate(11, 2019);
call datamart.contact_historization_aggregate(12, 2019);

/* ***************************** LOG PART ********************************* */
tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('2019 FINISH. Execution time: '||duration);
tstart := clock_timestamp();
CALL public.log_message('2020 : ');
/* ************************************************************************ */

call datamart.contact_historization_aggregate(1, 2020);  
call datamart.contact_historization_aggregate(2, 2020);  
call datamart.contact_historization_aggregate(3, 2020);   
call datamart.contact_historization_aggregate(4, 2020);
call datamart.contact_historization_aggregate(5, 2020);
call datamart.contact_historization_aggregate(6, 2020);
call datamart.contact_historization_aggregate(7, 2020);
call datamart.contact_historization_aggregate(8, 2020);
call datamart.contact_historization_aggregate(9, 2020);
call datamart.contact_historization_aggregate(10, 2020);
call datamart.contact_historization_aggregate(11, 2020);
call datamart.contact_historization_aggregate(12, 2020);

/* ***************************** LOG PART ********************************* */
tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('2020 FINISH. Execution time: '||duration);
tstart := clock_timestamp();
CALL public.log_message('2021 : ');
/* ************************************************************************ */

call datamart.contact_historization_aggregate(1, 2021);  
call datamart.contact_historization_aggregate(2, 2021);  
call datamart.contact_historization_aggregate(3, 2021);   
call datamart.contact_historization_aggregate(4, 2021);
call datamart.contact_historization_aggregate(5, 2021);
call datamart.contact_historization_aggregate(6, 2021);
call datamart.contact_historization_aggregate(7, 2021);
call datamart.contact_historization_aggregate(8, 2021);
call datamart.contact_historization_aggregate(9, 2021);
call datamart.contact_historization_aggregate(10, 2021);
call datamart.contact_historization_aggregate(11, 2021);
call datamart.contact_historization_aggregate(12, 2021);

-- Table: datamart.suivi_contact

-- 

	
	

tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('INIT COMPLETE. Execution time: '||duration);
duration := tend - fulltimestart;
CALL public.log_message('COMPLETE. Full execution time: '||duration);
CALL public.log_message('CORRECTLY FINISH.');

END
$BODY$;
