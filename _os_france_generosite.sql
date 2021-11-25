-- PROCEDURE: public._os_france_generosite()

-- DROP PROCEDURE public._os_france_generosite();

CREATE OR REPLACE PROCEDURE public._os_france_generosite(
	)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE tstart TIMESTAMP;
DECLARE fulltimestart TIMESTAMP; 
DECLARE tend TIMESTAMP;
DECLARE duration INTERVAL;
BEGIN

CALL public.log_message('Clean');
CALL public.log_message('Get primary campaign from opportunity');
tstart := clock_timestamp();
fulltimestart := clock_timestamp();

drop table if exists fg_contact;
CREATE TABLE fg_contact (
	"ContactId" VARCHAR(18) PRIMARY KEY,
	"Personne morale" INT,
	"Civilité" VARCHAR(120),
	"Prénom" VARCHAR(120),
	"Adresse Complete" VARCHAR(765),
	"Code postal" VARCHAR(120),
	"Code suspension adresse" INT,
	"Stop mailing" INT,
	"Date premier don" DATE,
	"Date premier PA" DATE,
	"Date dernier PA" DATE,
	"Date arrêt PA" DATE,
	"Code Banque"  VARCHAR(120), -- ?? From RegularGiving BIC? ou FR-BBBB
	"Périodicité" VARCHAR(1),
	"Montant PA" NUMERIC(15,2),
	"Origine PA" VARCHAR(120), -- ???
	"top email" INT,
	"top téléphone" INT,
	"top décédé" INT,
	"année de naissance" INT
);
CREATE INDEX contact_originepa_idx ON fg_contact("Origine PA");

drop table if exists fg_dons;
CREATE TABLE fg_dons (
	"ContactId" VARCHAR(18),
	"OpportunityId" VARCHAR(18) PRIMARY KEY,
	"Code mailing" VARCHAR(18), -- ???
	"Date" DATE,
	"Montant don" NUMERIC(15,2),
	"Code règlement" VARCHAR(30), -- select max(length("s360a__PaymentMethod__c")) from salesforce."s360a__Transaction2__c" => 16
	"Affectation" VARCHAR(120), -- ???
	"Entité"  VARCHAR(120), --  ???
	"Type de don" VARCHAR(50)
);
CREATE INDEX dons_reglement_idx ON fg_dons("Code règlement");
CREATE INDEX dons_codemailing_idx ON fg_dons("Code mailing");

drop table if exists fg_media;
CREATE TABLE fg_media (
	"Code media" VARCHAR(18) PRIMARY KEY,
	"Libellé" VARCHAR(256),
	"Média" VARCHAR(64)
);

/* ***************************** LOG PART ********************************* */
tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('Tables créées. Execution time: '||duration);
tstart := clock_timestamp();
CALL public.log_message('fg_dons');
/* ************************************************************************ */

DELETE FROM fg_dons;

INSERT
INTO fg_dons (
	"ContactId", "OpportunityId", "Code mailing", "Date", "Montant don", "Code règlement", "Affectation", "Entité", "Type de don" )
(
	SELECT
		"Opportunity"."s360a__Contact__c",
		"Opportunity"."Id",
		"Opportunity"."CampaignId", -- Code mailing
		"Opportunity"."CloseDate",
		"Opportunity"."Amount",
		"Opportunity"."Payment_Method_OPP__c",
		NULL, -- Affectation p.ex. 'Unrestricted Donation'
		NULL, -- Entité
		"RecordType"."DeveloperName" -- Type de don
	FROM salesforce."Opportunity"
	LEFT JOIN
		salesforce."s360a__RegularGiving__c"
	        ON "Opportunity"."s360a__RegularGiving__c" = "s360a__RegularGiving__c"."Id"
	LEFT JOIN
		salesforce."RecordType"
		ON "Opportunity"."RecordTypeId" = "RecordType"."Id"
	WHERE "IsClosed"
		AND "IsWon"
		AND "CloseDate" >= '2020-12-01'::DATE
		AND COALESCE("Opportunity"."gpi__GiftCountryOfOwnership__c", "s360a__RegularGiving__c"."gpi__CountryOfOwnership__c") = 'France'
);
-- INSERT 0 16078741
--VACUUM ANALYZE fg_dons;

-- select distinct "s360a__PaymentMethod__c" from salesforce."s360a__Transaction2__c";
UPDATE fg_dons
SET
	"Code règlement" = "s360a__PaymentMethod__c"
FROM (
	SELECT
		"s360a__Opportunity__c",
		"s360a__PaymentMethod__c"
	FROM salesforce."s360a__Transaction2__c"
	WHERE "s360a__Amount__c" > 0
	ORDER BY "CreatedDate"
) AS subquery
WHERE fg_dons."OpportunityId" = subquery."s360a__Opportunity__c"
AND "Code règlement" IS NULL
;
-- UPDATE 15498844

UPDATE fg_dons
SET
	"Code règlement" = 'Direct Debit'
WHERE
	EXTRACT(year FROM "Date") < '2010'
	AND "Code règlement" IS NULL
	AND "Type de don" = 'Gift_Recurring'
;
-- UPDATE 521044

-- select SUM("Montant don"), EXTRACT (year FROM "Date") AS YEAR FROM fg_dons GROUP BY EXTRACT (year FROM "Date") ORDER BY year;
-- select SUM("Montant don") AS sum_2019, "Code règlement" FROM fg_dons WHERE EXTRACT (year FROM "Date")='2019' GROUP BY "Code règlement" ;
-- select SUM("Montant don") AS sum_2019, "Code règlement", "Type de don" FROM fg_dons WHERE EXTRACT (year FROM "Date")='2019' GROUP BY "Code règlement", "Type de don" ;
-- select EXTRACT (year FROM "Date") AS year, SUM("Montant don") AS sum, "Code règlement", "Type de don" FROM fg_dons GROUP BY year, "Code règlement", "Type de don" ;
-- select "Payment_Method_OPP__c", COUNT(*) from salesforce."Opportunity" GROUP BY "Payment_Method_OPP__c";

DELETE
FROM
	fg_dons
WHERE "Type de don" = 'Magazine_Subscription';
-- DELETE 29

-- SELECT * FROM salesforce."OpportunityLineItem" JOIN salesforce."Product2" ON "OpportunityLineItem"."Product2Id" = "Product2"."Id" LIMIT 1000;
UPDATE fg_dons
SET
	"Affectation" = sub."Name"
FROM (
	SELECT
		"OpportunityLineItem"."OpportunityId",
		"Product2"."Name"
	FROM
		salesforce."OpportunityLineItem"
	JOIN 
		salesforce."Product2"
		ON "OpportunityLineItem"."Product2Id" = "Product2"."Id"
) AS sub
WHERE
	fg_dons."OpportunityId" = sub."OpportunityId"
;
-- UPDATE 15983857

/* ***************************** LOG PART ********************************* */
tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('fg_dons terminé. Execution time: '||duration);
tstart := clock_timestamp();
CALL public.log_message('fg_contact');
/* ************************************************************************ */

DELETE FROM fg_contact;
-- DELETE 649579

INSERT
INTO fg_contact
("ContactId", "Civilité", "Personne morale", "Prénom", "Adresse Complete", "Code postal", "Code suspension adresse", "Stop mailing", "Date premier PA", "Date dernier PA", "Date arrêt PA", "Code Banque", "Périodicité", "Montant PA", "Origine PA", "top email", "top téléphone", "top décédé", "année de naissance")
(
	SELECT
		"Id",
		"Salutation",
		CASE
			WHEN "Salutation" = 'Société' OR "Salutation" = 'Association'
				THEN 1
			ELSE
				0
		END,
		CASE
			WHEN "Salutation" = 'Société' OR "Salutation" = 'Association'
				THEN NULL
			ELSE
				"FirstName"
		END,
		"s360a__AddressPreferredMailingAddressMultiLine__c" AS "Adresse Complete",
		CASE
			WHEN "MailingCountry" = 'France'
				THEN "MailingPostalCode"
			ELSE
				"MailingCountry"
		END AS code_postal,
		CASE
			WHEN "RTSCountPrimary__c"::INT > 2
				THEN 1
			ELSE
				0
		END,
		CASE
			WHEN "Terminated__c" OR "s360a__DoNotContact__c" OR NOT "s360a__CommunicateViaMail__c"
				THEN 1
			ELSE
				0
		END,
		NULL,  -- Date premier PA
		NULL,  -- Date dernier PA
		NULL,  -- Date arrêt PA
		NULL,  -- Code Banque
		NULL,  -- Périodicité
		NULL,  -- Montant PA
		NULL,  -- Origine PA
		CASE WHEN "Email" IS NOT NULL AND "Email" <> ''
			THEN 1
		     ELSE
			0
		END,
		CASE WHEN "Phone" IS NOT NULL AND "Phone" <> ''
			THEN 1
		     ELSE
			0
		END,
		CASE WHEN "s360a__Deceased__c"
			THEN 1
		     ELSE
			0
		END,
		EXTRACT(YEAR FROM "Birthdate") AS birth_year
		
	FROM salesforce."Contact"
	where "Id" in (select * from distinct_contact) 
);
-- INSERT 0 650027

-- select "s360a__BIC__c",  substring(replace("s360a__BIC__c",' ','') FROM 1 FOR 6) FROM salesforce."s360a__AutomatedTransaction__c" WHERE substring("s360a__BIC__c" FROM 5 FOR 2)<>'FR';

UPDATE
        fg_contact
SET
        "Périodicité" = freq,
        "Montant PA" = amount,
	"Code Banque" = first_rg_data."Code Banque"
FROM (
	SELECT
		"s360a__RegularGiving__c"."s360a__Contact__c" AS "ContactId",
		"s360a__RegularGiving__c"."s360a__Amount__c" AS "amount",
		CASE
			WHEN "s360a__RegularGiving__c"."s360a__TransactionsPerYear__c" = 12
				THEN 'M'
			WHEN "s360a__RegularGiving__c"."s360a__TransactionsPerYear__c" = 6
				THEN 'S'
			WHEN "s360a__RegularGiving__c"."s360a__TransactionsPerYear__c" = 4
				THEN 'T'
			WHEN "s360a__RegularGiving__c"."s360a__TransactionsPerYear__c" = 2
				THEN 'B'
			WHEN "s360a__RegularGiving__c"."s360a__TransactionsPerYear__c" = 1
				THEN 'A'
			ELSE
				'?'
		END AS freq,
		"Code Banque"
	FROM
		salesforce."s360a__RegularGiving__c"
	JOIN (
		SELECT MIN("Id") AS minid
		FROM salesforce."s360a__RegularGiving__c"
		WHERE "s360a__TransactionsPerYear__c" > 0
			AND (
				   "s360a__RGStatus__c" = 'Initial'
                        	OR "s360a__RGStatus__c" = 'Active'
                        	OR "s360a__RGStatus__c" = 'Last Attempt Failed'
                        	OR "s360a__RGStatus__c" = 'Held'
			)

		GROUP BY "s360a__Contact__c"
	) AS first_rg
		ON "s360a__RegularGiving__c"."Id" = first_rg.minid -- 259206
	LEFT JOIN (
		SELECT MIN("Id") AS atid, "s360a__RegularGiving__c"
		FROM salesforce."s360a__AutomatedTransaction__c"
		GROUP BY "s360a__RegularGiving__c"
	) AS first_at
		ON first_at."s360a__RegularGiving__c" = first_rg.minid
	LEFT JOIN (
		SELECT "Id", substring(replace("s360a__BIC__c",' ','') FROM 1 FOR 6) AS "Code Banque"
		FROM salesforce."s360a__AutomatedTransaction__c"
	) AS first_at2
		ON first_at.atid = first_at2."Id"
) AS first_rg_data
WHERE
        fg_contact."ContactId" = first_rg_data."ContactId"
;
-- UPDATE 198624

-- Mise à jour permier don

UPDATE
	fg_contact
SET
	"Date premier don" = first_don
FROM (
	SELECT
		"Opportunity"."s360a__Contact__c",
		MIN("Opportunity"."CloseDate") AS first_don
	FROM
		salesforce."Opportunity"
	LEFT JOIN
		salesforce."s360a__RegularGiving__c"
	        ON "Opportunity"."s360a__RegularGiving__c" = "s360a__RegularGiving__c"."Id"
	WHERE
		"IsClosed"
		AND "IsWon"
		AND COALESCE("Opportunity"."gpi__GiftCountryOfOwnership__c", "s360a__RegularGiving__c"."gpi__CountryOfOwnership__c") = 'France'
	GROUP BY 
		"Opportunity"."s360a__Contact__c"
) AS updates
WHERE
	fg_contact."ContactId" = updates."s360a__Contact__c"
;
-- UPDATE 564971

-- Mise à jour permier / dernier PA

UPDATE
	fg_contact
SET
	"Date premier PA" = first_pa,
	"Date dernier PA" = last_pa
FROM (
	SELECT
		"Opportunity"."s360a__Contact__c",
		MIN("Opportunity"."CloseDate") AS first_pa,
		MAX("Opportunity"."CloseDate") AS last_pa
	FROM
		salesforce."Opportunity"
	LEFT JOIN
		salesforce."s360a__RegularGiving__c"
	        ON "Opportunity"."s360a__RegularGiving__c" = "s360a__RegularGiving__c"."Id"
	WHERE
		"Opportunity"."RecordTypeId" = (SELECT "Id" FROM salesforce."RecordType" WHERE "DeveloperName"='Gift_Recurring')
		AND "IsClosed"
		AND "IsWon"
		AND COALESCE("Opportunity"."gpi__GiftCountryOfOwnership__c", "s360a__RegularGiving__c"."gpi__CountryOfOwnership__c") = 'France'
	GROUP BY 
		"Opportunity"."s360a__Contact__c"
) AS updates
WHERE
	fg_contact."ContactId" = updates."s360a__Contact__c"
;
-- UPDATE 412177

-- Recherche du "Origine PA"

UPDATE
	fg_contact
SET
	"Origine PA" = updates."CampaignId"
FROM (
	SELECT
		result."s360a__Contact__c",
		result."CampaignId"
	FROM
		salesforce."Opportunity" AS result
	JOIN (
		SELECT
			"Opportunity"."s360a__Contact__c" AS "ContactId",
			MIN("Opportunity"."Id") AS oldest_opportunity_id
		FROM
			salesforce."Opportunity"
		JOIN (
			SELECT
				"Opportunity"."s360a__Contact__c" AS "ContactId",
				MIN("Opportunity"."CloseDate") AS oldest_date
			FROM
				salesforce."Opportunity"
			LEFT JOIN
				salesforce."s360a__RegularGiving__c"
			        ON "Opportunity"."s360a__RegularGiving__c" = "s360a__RegularGiving__c"."Id"
			WHERE
				"Opportunity"."RecordTypeId" = (SELECT "Id" FROM salesforce."RecordType" WHERE "DeveloperName"='Gift_Recurring')
				AND "IsClosed"
				AND "IsWon"
				AND COALESCE("Opportunity"."gpi__GiftCountryOfOwnership__c", "s360a__RegularGiving__c"."gpi__CountryOfOwnership__c") = 'France'
			GROUP BY
				"Opportunity"."s360a__Contact__c"
		) AS oldest_dates
			ON oldest_dates."ContactId" = "Opportunity"."s360a__Contact__c"
			AND "Opportunity"."CloseDate" = oldest_dates.oldest_date
		LEFT JOIN
			salesforce."s360a__RegularGiving__c"
		        ON "Opportunity"."s360a__RegularGiving__c" = "s360a__RegularGiving__c"."Id"
		WHERE
			"Opportunity"."RecordTypeId" = (SELECT "Id" FROM salesforce."RecordType" WHERE "DeveloperName"='Gift_Recurring')
			AND "IsClosed"
			AND "IsWon"
			AND COALESCE("Opportunity"."gpi__GiftCountryOfOwnership__c", "s360a__RegularGiving__c"."gpi__CountryOfOwnership__c") = 'France'
		GROUP BY
			"Opportunity"."s360a__Contact__c"
	) AS oldest_pa_id
		ON result."Id" = oldest_pa_id.oldest_opportunity_id
) AS updates
WHERE
	fg_contact."ContactId" = updates."s360a__Contact__c"
;
-- UPDATE 412177

-- Mise à jour de la date d'arrêt de PA
UPDATE
	fg_contact
SET
	"Date arrêt PA" = updates.max_cancel_date
FROM (
	SELECT
		"s360a__Contact__c",
		MAX("gpi__Cancelled_Date__c") AS max_cancel_date
	FROM
		salesforce."s360a__RegularGiving__c"
	WHERE
		"gpi__CountryOfOwnership__c" = 'France'
	GROUP BY
		"s360a__Contact__c"
) AS updates
WHERE
	fg_contact."ContactId" = updates."s360a__Contact__c"
;
-- UPDATE 247084

/* ***************************** LOG PART ********************************* */
tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('fg_contact terminé. Execution time: '||duration);
tstart := clock_timestamp();
CALL public.log_message('fg_media');
/* ************************************************************************ */

DELETE FROM fg_media;

-- INSERT INTO fg_media (
-- 	"Code media",
-- 	"Libellé",
-- 	"Média")
-- (
-- 	SELECT DISTINCT 
-- 	       "Campaign"."Id",
-- 	       "Campaign"."Name",
-- 	       CASE
-- 		WHEN "Campaign"."gpi__GP_Channel__c" IS NOT NULL
-- 			THEN "Campaign"."gpi__GP_Channel__c"
-- 		WHEN "Campaign"."gpi__Programme__c" = 'DDC'
-- 			THEN 'DDC'
-- 		WHEN "Campaign"."s360a__CampaignCode__c" LIKE 'J__J%'
-- 			THEN 'Offline'
-- 		WHEN "Campaign"."s360a__CampaignCode__c" LIKE 'J__E%'
-- 			THEN 'Online'
-- 		WHEN "Campaign"."Name" LIKE 'FR DD %' OR "Campaign"."Name" LIKE 'DD FR %' 
-- 			THEN 'DDC'
-- 		WHEN "Campaign"."Name" LIKE 'FR WEB %'
-- 			THEN 'Online'
-- 		WHEN "Campaign"."Name" LIKE 'D160A%lux%'
-- 			THEN 'DDC'
-- 		WHEN "Campaign"."Name" LIKE 'FR NC % Courrier%'
-- 			THEN 'Offline'
-- 		WHEN "Campaign"."Name" LIKE 'FR TMK%'
-- 			THEN 'Telephone'
-- 		WHEN "Campaign"."Name" LIKE 'FR SA %'
-- 			THEN 'Offline'
-- 		WHEN "Campaign"."Name" LIKE 'FR %MAGAZINE'
-- 			THEN 'Offline'
-- 		WHEN "Campaign"."Name" LIKE 'FR %Receipts%'
-- 			THEN 'Offline'
-- 		WHEN "Campaign"."Name" LIKE 'FR RA %Mailing%'
-- 			THEN 'Online'
-- 		WHEN "Campaign"."Name" LIKE 'FR LEGS %Offline%'
-- 			THEN 'Offline'
-- 		ELSE
-- 			NULL
-- 	       END AS "Media"
-- 	FROM fg_dons
-- 	JOIN salesforce."Campaign"
-- 	        ON "Campaign"."Id" = fg_dons."Code mailing"
-- )
-- ;
INSERT INTO fg_media (
	"Code media",
	"Libellé",
	"Média")
(
	SELECT DISTINCT 
	       "Campaign"."Id",
	       "Campaign"."Name",
	       CASE
		WHEN "Campaign"."gpi__GP_Channel__c" IS NOT NULL
			THEN "Campaign"."gpi__GP_Channel__c"
		WHEN "Campaign"."gpi__Programme__c" = 'DDC'
			THEN 'DDC'
		WHEN "Campaign"."s360a__CampaignCode__c" LIKE 'J__J%'
			THEN 'Offline'
		WHEN "Campaign"."s360a__CampaignCode__c" LIKE 'J__E%'
			THEN 'Online'
		WHEN "Campaign"."Name" LIKE 'FR DD %' OR "Campaign"."Name" LIKE 'DD FR %' 
			THEN 'DDC'
		WHEN "Campaign"."Name" LIKE 'FR WEB %'
			THEN 'Online'
		WHEN "Campaign"."Name" LIKE 'D160A%lux%'
			THEN 'DDC'
		WHEN "Campaign"."Name" LIKE 'FR NC % Courrier%'
			THEN 'Offline'
		WHEN "Campaign"."Name" LIKE 'FR TMK%'
			THEN 'Telephone'
		WHEN "Campaign"."Name" LIKE 'FR SA %'
			THEN 'Offline'
		WHEN "Campaign"."Name" LIKE 'FR %MAGAZINE'
			THEN 'Offline'
		WHEN "Campaign"."Name" LIKE 'FR %Receipts%'
			THEN 'Offline'
		WHEN "Campaign"."Name" LIKE 'FR RA %Mailing%'
			THEN 'Online'
		WHEN "Campaign"."Name" LIKE 'FR LEGS %Offline%'
			THEN 'Offline'
		ELSE
			NULL
	       END AS "Media"
	FROM (
		SELECT fg_dons."Code mailing" AS campaign_id FROM fg_dons
		UNION
		SELECT fg_contact."Origine PA" AS campaign_id FROM fg_contact
	) AS used_campaigns
	JOIN salesforce."Campaign"
	        ON "Campaign"."Id" = used_campaigns.campaign_id
)
;
-- INSERT 0 5132

-- SELECT DISTINCT "Campaign"."s360a__CampaignCode__c", COUNT(*)
-- FROM fg_dons
-- JOIN salesforce."Campaign"
--         ON "Campaign"."Id" = dons."Code mailing"
-- GROUP BY "Campaign"."s360a__CampaignCode__c"
-- ;
-- 
-- 
-- SELECT DISTINCT "Campaign"."Id", "Campaign"."Name", "Campaign"."s360a__CampaignCode__c", "Campaign"."s360a__RecruitmentType__c", "Campaign"."s360a__SubType__c", "External_ID__c", "gpi__GP_Channel__c", "gpi__Programme__c", "gpi__Sub_Channel__c"
-- FROM fg_dons
-- JOIN salesforce."Campaign"
--         ON "Campaign"."Id" = dons."Code mailing"
-- ORDER BY "Campaign"."Name"
-- ;

tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('Contact histo COMPLETE. Execution time: '||duration);
duration := tend - fulltimestart;
CALL public.log_message('COMPLETE. Full execution time: '||duration);
CALL public.log_message('CORRECTLY FINISH.');

END
$BODY$;
