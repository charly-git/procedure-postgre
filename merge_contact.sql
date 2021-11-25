-- PROCEDURE: public.merge_contact()

-- DROP PROCEDURE public.merge_contact();

CREATE OR REPLACE PROCEDURE public.merge_contact(
	)
LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
/*

-- SELECT COUNT > 1 GROUP BY "Contact"."FirstName"||"Contact"."LastName"||"Contact"."s360a__EmailAddressPersonalEmailAddress__c"
SELECT LOWER(TRANSLATE("Contact"."FirstName"||"Contact"."LastName"||"Contact"."s360a__EmailAddressPersonalEmailAddress__c",'çéèêëàâäùûüçôöîïÿ-','ceeeeaaauuucooiiy ')),
count(LOWER(TRANSLATE("Contact"."FirstName"||"Contact"."LastName"||"Contact"."s360a__EmailAddressPersonalEmailAddress__c",'çéèêëàâäùûüçôöîïÿ-','ceeeeaaauuucooiiy '))) FROM Salesforce."Contact"
GROUP BY LOWER(TRANSLATE("Contact"."FirstName"||"Contact"."LastName"||"Contact"."s360a__EmailAddressPersonalEmailAddress__c",'çéèêëàâäùûüçôöîïÿ-','ceeeeaaauuucooiiy '))
HAVING count(LOWER(TRANSLATE("Contact"."FirstName"||"Contact"."LastName"||"Contact"."s360a__EmailAddressPersonalEmailAddress__c",'çéèêëàâäùûüçôöîïÿ-','ceeeeaaauuucooiiy '))) > 1

-- SELECT COUNT > 1 GROUP BY "Contact"."FirstName"||"Contact"."LastName"||"Contact"."s360a__PhoneNumberMobilePhoneNumber__c"
SELECT LOWER(TRANSLATE("Contact"."FirstName"||"Contact"."LastName"||"Contact"."s360a__PhoneNumberMobilePhoneNumber__c",'çéèêëàâäùûüçôöîïÿ-','ceeeeaaauuucooiiy ')),
count(LOWER(TRANSLATE("Contact"."FirstName"||"Contact"."LastName"||"Contact"."s360a__PhoneNumberMobilePhoneNumber__c",'çéèêëàâäùûüçôöîïÿ-','ceeeeaaauuucooiiy '))) FROM Salesforce."Contact"
GROUP BY LOWER(TRANSLATE("Contact"."FirstName"||"Contact"."LastName"||"Contact"."s360a__PhoneNumberMobilePhoneNumber__c",'çéèêëàâäùûüçôöîïÿ-','ceeeeaaauuucooiiy '))
HAVING count(LOWER(TRANSLATE("Contact"."FirstName"||"Contact"."LastName"||"Contact"."s360a__PhoneNumberMobilePhoneNumber__c",'çéèêëàâäùûüçôöîïÿ-','ceeeeaaauuucooiiy '))) > 1

-- SELECT INTO TABLE Contact_MergeTest
SELECT LOWER(TRANSLATE("Contact"."FirstName"||"Contact"."LastName"||"Contact"."s360a__EmailAddressPersonalEmailAddress__c",'çéèêëàâäùûüçôöîïÿ-','ceeeeaaauuucooiiy ')),
count(LOWER(TRANSLATE("Contact"."FirstName"||"Contact"."LastName"||"Contact"."s360a__EmailAddressPersonalEmailAddress__c",'çéèêëàâäùûüçôöîïÿ-','ceeeeaaauuucooiiy '))) 
INTO public.Contact_MergeTest FROM Salesforce."Contact"
GROUP BY LOWER(TRANSLATE("Contact"."FirstName"||"Contact"."LastName"||"Contact"."s360a__EmailAddressPersonalEmailAddress__c",'çéèêëàâäùûüçôöîïÿ-','ceeeeaaauuucooiiy '))
HAVING count(LOWER(TRANSLATE("Contact"."FirstName"||"Contact"."LastName"||"Contact"."s360a__EmailAddressPersonalEmailAddress__c",'çéèêëàâäùûüçôöîïÿ-','ceeeeaaauuucooiiy '))) > 1

-- UPDATE Table Contact_MergeTest ConstituentId_1 / ConstituentId_2
UPDATE public.Contact_MergeTest SET "ConstituentId_1" = (SELECT MIN(CAST("Contact"."gpi__GP_Constituent_ID__c" AS INTEGER)) FROM Salesforce."Contact" WHERE
LOWER(TRANSLATE("Contact"."FirstName"||"Contact"."LastName"||"Contact"."s360a__EmailAddressPersonalEmailAddress__c",'çéèêëàâäùûüçôöîïÿ-','ceeeeaaauuucooiiy ')) = Contact_MergeTest."lower")

UPDATE public.Contact_MergeTest SET "ConstituentId_2" = (SELECT MAX(CAST("Contact"."gpi__GP_Constituent_ID__c"AS INTEGER)) FROM Salesforce."Contact" WHERE
LOWER(TRANSLATE("Contact"."FirstName"||"Contact"."LastName"||"Contact"."s360a__EmailAddressPersonalEmailAddress__c",'çéèêëàâäùûüçôöîïÿ-','ceeeeaaauuucooiiy ')) = Contact_MergeTest."lower")

-- Insertion de données de test	
	INSERT INTO public."Contact_Merge"(
	"ConstituentId_A", "ConstituentId_B", "ConstituentId_1", "ConstituentId_2", "ContactId_1", "ContactId_2", "AccountId_1", "AccountId_2")
	VALUES ('3191308','3191307',NULL,NULL,NULL,NULL,NULL,NULL);
	
-- Ajout colonne ALTER TABLE public."Contact_Merge" ADD Column "ConstituentId_2" VARCHAR(30)
	
-- RENAME COLUMN "gpi__GP_Constituent_ID__c"  ALTER TABLE public."Contact_Merge" RENAME COLUMN "gpi__GP_Constituent_ID__c" TO "ConstituentId_1";
	
	SELECT *
	FROM public."Contact_Merge";
	
-- Insertion de données de test ConstituentId_2 
	UPDATE public."Contact_Merge" SET "ConstituentId_2" = (SELECT "Contact"."gpi__GP_Constituent_ID__c" 
	FROM Salesforce."Contact" WHERE "Contact"."gpi__GP_Constituent_ID__c" = '1917756')

-- Ajout des colonnes ContactId et AccountId
	ALTER TABLE public.Contact_Merge ADD Column "ConstituentId_1" VARCHAR(30);
	ALTER TABLE public.Contact_Merge ADD Column "ConstituentId_2" VARCHAR(30);
	ALTER TABLE public.Contact_Merge ADD Column "ContactId_1" VARCHAR(30);
	ALTER TABLE public.Contact_Merge ADD Column "ContactId_2" VARCHAR(30);
	ALTER TABLE public.Contact_Merge ADD Column "AccountId_1" VARCHAR(30);
	ALTER TABLE public.Contact_Merge ADD Column "AccountId_2" VARCHAR(30)

-- Select table Contact_Merge
	SELECT "ConstituentId_A", "ConstituentId_B", "ConstituentId_1", "ConstituentId_2", "ContactId_1", "ContactId_2", "AccountId_1", "AccountId_2" FROM public."Contact_Merge";

-- Update ContactId et AccountId LEAST / GREATEST ConstituentId

	UPDATE public."Contact_Merge" SET "ConstituentId_1" = LEAST("Contact_Merge"."ConstituentId_A", "Contact_Merge"."ConstituentId_B");
	UPDATE public."Contact_Merge" SET "ConstituentId_2" = GREATEST("Contact_Merge"."ConstituentId_A", "Contact_Merge"."ConstituentId_B");
	
	UPDATE public."Contact_Merge" SET "ContactId_1" = (SELECT "Contact"."Id" FROM Salesforce."Contact"
	WHERE "Contact"."gpi__GP_Constituent_ID__c" = "Contact_Merge"."ConstituentId_1");
	
	UPDATE public."Contact_Merge" SET "AccountId_1" = (SELECT "Contact"."AccountId" FROM Salesforce."Contact"
	WHERE "Contact"."gpi__GP_Constituent_ID__c" = "Contact_Merge"."ConstituentId_1");
	
	UPDATE public."Contact_Merge" SET "ContactId_2" = (SELECT "Contact"."Id" FROM Salesforce."Contact"
	WHERE "Contact"."gpi__GP_Constituent_ID__c" = "Contact_Merge"."ConstituentId_2");
	
	UPDATE public."Contact_Merge" SET "AccountId_2" = (SELECT "Contact"."AccountId" FROM Salesforce."Contact"
	WHERE "Contact"."gpi__GP_Constituent_ID__c" = "Contact_Merge"."ConstituentId_2");

/*	-- Relation à définir
	UPDATE public."Contact_Merge" SET "ContactId_1" = subquery."Id", "AccountId_1" = subquery."AccountId"
	FROM (SELECT "Contact"."Id" AS "Id", "Contact"."AccountId" AS "AccountId" FROM Salesforce."Contact"
	WHERE "Contact"."gpi__GP_Constituent_ID__c" IN (LEAST("Contact_Merge"."ConstituentId_1", "Contact_Merge"."ConstituentId_2")) AS subquery

	UPDATE public."Contact_Merge" SET "ContactId_2" = subquery."Id", "AccountId_2" = subquery."AccountId"
	FROM (SELECT "Contact"."Id" AS "Id", "Contact"."AccountId" AS "AccountId" FROM Salesforce."Contact"
	WHERE "Contact"."gpi__GP_Constituent_ID__c" IN (
	SELECT GREATEST("Contact_Merge"."ConstituentId_1", "Contact_Merge"."ConstituentId_2") FROM public."Contact_Merge")) AS subquery
*/

-- Update Stagings
SELECT "s360aie__Staging__c"."Id", "s360aie__Staging__c"."s360aie__ContactID__c","s360aie__Staging__c"."s360aie__CampaignMemberId__c" 
INTO public."Staging_MergeContactId" FROM Salesforce."s360aie__Staging__c" WHERE "s360aie__Staging__c"."s360aie__ContactID__c" IN (
SELECT "Contact_Merge"."ContactId_2" FROM public."Contact_Merge")

UPDATE public."Staging_MergeContactId" SET "s360aie__CampaignMemberId__c" = NULL

UPDATE public."Staging_MergeContactId" SET "s360aie__ContactID__c" = (
	SELECT "Contact_Merge"."ContactId_1" FROM public."Contact_Merge" WHERE 
	"Contact_Merge"."ContactId_2" = "Staging_MergeContactId"."s360aie__ContactID__c")

-- Update Regular Giving
SELECT "s360a__RegularGiving__c"."Id", "s360a__RegularGiving__c"."s360a__Account__c", "s360a__RegularGiving__c"."s360a__Contact__c" 
INTO public."RegularGiving_MergeContactId" FROM Salesforce."s360a__RegularGiving__c" WHERE "s360a__RegularGiving__c"."s360a__Contact__c" IN (
SELECT "Contact_Merge"."ContactId_2" FROM public."Contact_Merge")

UPDATE public."RegularGiving_MergeContactId" SET "s360a__Contact__c" = (
	SELECT "Contact_Merge"."ContactId_1" FROM public."Contact_Merge" WHERE 
	"Contact_Merge"."ContactId_2" = "RegularGiving_MergeContactId"."s360a__Contact__c")

UPDATE public."RegularGiving_MergeContactId" SET "s360a__Account__c" = (
	SELECT "Contact_Merge"."AccountId_1" FROM public."Contact_Merge" WHERE 
	"Contact_Merge"."AccountId_2" = "RegularGiving_MergeContactId"."s360a__Account__c")

-- Update Opportunities
SELECT "Opportunity"."Id", "Opportunity"."AccountId", "Opportunity"."s360a__Contact__c" 
INTO public."Opportunity_MergeContactId" FROM Salesforce."Opportunity" WHERE "Opportunity"."s360a__Contact__c" IN (
SELECT "Contact_Merge"."ContactId_2" FROM public."Contact_Merge")

UPDATE public."Opportunity_MergeContactId" SET "s360a__Contact__c" = (
	SELECT "Contact_Merge"."ContactId_1" FROM public."Contact_Merge" WHERE 
	"Contact_Merge"."ContactId_2" = "Opportunity_MergeContactId"."s360a__Contact__c");
	
UPDATE public."Opportunity_MergeContactId" SET "AccountId" = (
	SELECT "Contact_Merge"."AccountId_1" FROM public."Contact_Merge" WHERE 
	"Contact_Merge"."AccountId_2" = "Opportunity_MergeContactId"."AccountId")

-- Insert & Delete Campaign Members
SELECT "CampaignMember"."ContactId", "CampaignMember"."CreatedDate", "CampaignMember"."CampaignId","CampaignMember"."s360aie__ResponseCode__c",
"CampaignMember"."s360aie__Outcome__c", "CampaignMember"."Outcome_Bis__c", "CampaignMember"."TFR_Call_Date__c", "CampaignMember"."TMK_Raison_Arret_PA__c",
"CampaignMember"."Opportunity_Promise_Date_Bis__c", "CampaignMember"."Opportunity_Promise_Amount_Bis__c", "CampaignMember"."gpi__Receipt_Date__c",
"CampaignMember"."gpi__Receipt_Date_Range__c", "CampaignMember"."gpi__Receipt_Amount__c", "CampaignMember"."gpi__Receipt_Number__c",
"CampaignMember"."gpi__Receipt_Payment_Methods__c", "CampaignMember"."gpi__Receipt_Number_of_Transactions__c", "CampaignMember"."Forme_du_don__c",
"CampaignMember"."Receipt_Gift_Date__c", "CampaignMember"."Nature_du_don__c" 
INTO public."CampaignMember_MergeContact_Insert" FROM Salesforce."CampaignMember" WHERE "CampaignMember"."ContactId" IN (
SELECT "Contact_Merge"."ContactId_2" FROM public."Contact_Merge")

-- Vérifier Date heure CreatedDate Campaign Member

-- DROP TABLE public."CampaignMember_MergeContact_Insert"

SELECT "CampaignMember"."Id"
INTO public."CampaignMember_MergeContact_Delete" FROM Salesforce."CampaignMember" WHERE "CampaignMember"."ContactId" IN (
SELECT "Contact_Merge"."ContactId_2" FROM public."Contact_Merge")

UPDATE public."CampaignMember_MergeContact_Insert" SET "ContactId" = (
	SELECT "Contact_Merge"."ContactId_1" FROM public."Contact_Merge" WHERE 
	"Contact_Merge"."ContactId_2" = "CampaignMember_MergeContact_Insert"."ContactId");

-- Update Cases
SELECT "Case"."Id", "Case"."ContactId", "Case"."AccountId" 
INTO public."Case_MergeContactId" FROM Salesforce."Case" WHERE "Case"."ContactId" IN (
SELECT "Contact_Merge"."ContactId_2" FROM public."Contact_Merge")

UPDATE public."Case_MergeContactId" SET "ContactId" = (
	SELECT "Contact_Merge"."ContactId_1" FROM public."Contact_Merge" WHERE 
	"Contact_Merge"."ContactId_2" = "Case_MergeContactId"."ContactId");

UPDATE public."Case_MergeContactId" SET "AccountId" = (
	SELECT "Contact_Merge"."AccountId_1" FROM public."Contact_Merge" WHERE 
	"Contact_Merge"."AccountId_2" = "Case_MergeContactId"."AccountId");
	
-- UPDATE First Contact & DELETE Second Contact

---- Table UPDATE First Contact
SELECT "Contact"."Id", "Contact"."Age_scored__c", "Contact"."s360a__DefaultCommunicationMethod__c", "Contact"."EventCommunicationsOK__c", 
"Contact"."s360a__InformalName__c", "Contact"."gpi__Last_Receipt_Date__c", "Contact"."Life_Stage_FR__c", "Contact"."gpi__original_utm_campaign__c", 
"Contact"."s360a__PhoneNumberOtherPhoneNumber__c", "Contact"."s360a__AddressPrimaryPreferredStreetAddress__c", "Contact"."Signup_Date__c",
"Contact"."s360a__PhoneNumberPreferredType__c", "Contact"."Birthdate", "Contact"."en_address_1__c", "Contact"."s360a__FormalName__c",
"Contact"."s360a__InitialContactSource__c", "Contact"."gpi__Latest_UTM_Campaign__c", "Contact"."gpi__LinkedInURL__c", "Contact"."gpi__original_utm_content__c",
"Contact"."s360a__PopulateUtility__c", "Contact"."gpi__Previous_Constituent_ID__c", "Contact"."Signup_Location__c", "Contact"."s360a__AddressPrimaryAddressType__c",
"Contact"."s360a__CommunicationCodes__c",  "Contact"."en_address_2__c", "Contact"."s360a__FrequencyRFM__c", "Contact"."L1_ContactAddressLine1__c",
"Contact"."gpi__Latest_UTM_Content__c", "Contact"."s360a__PhoneNumberMobilePhoneNumber__c", "Contact"."gpi__original_utm_medium__c",
"Contact"."gpi__PreferredLanguage__c", "Contact"."gpi__Primary_Campaign__c", "Contact"."gpi__TotalNumberOfDonationsOffSystem__c", "Contact"."gpi__LegacyStage__c",
"Contact"."s360a__ContactCodes__c", "Contact"."en_address_3__c", "Contact"."s360a__FullName__c", "Contact"."L2_ContactAddressLine2__c",
"Contact"."gpi__Latest_UTM_Medium__c", "Contact"."gpi__Modified_On__c", "Contact"."gpi__original_utm_source__c",
"Contact"."s360a__AddressPrimaryPreferredMailingAddress__c", "Contact"."s360a__RecencyRFM__c", "Contact"."OwnerId",
"Contact"."DisplayedAutomaticContactCodes__c", "Contact"."EN_Email__c", "Contact"."s360a__Gender__c", "Contact"."L3_ContactAddressLine3__c",
"Contact"."gpi__Latest_UTM_Source__c", "Contact"."s360a__MonetaryRFM__c", "Contact"."gpi__original_utm_term__c", "Contact"."s360a__RFMScore__c",
"Contact"."gpi__UTM_Last_Updated__c", "Contact"."s360a__CreateOne2OneAccount__c", "Contact"."EN_PET__c", "Contact"."Gender_scored__c",
"Contact"."L4_ContactAddressLine4__c", "Contact"."gpi__Latest_UTM_Term__c",  "Contact"."N1__c", "Contact"."s360a__EmailAddressWorkEmailAddress__c",
"Contact"."EN_Connector__EN_Supporter_ID__c", "Contact"."gpi__GiftCompliance__c", "Contact"."N2__c","Contact"."N3__c", "Contact"."N4__c", "Contact"."N5__c",
"Contact"."s360a__MergeStatus__c", "Contact"."s360a__MergeMessage__c"
INTO public."Contact_Merge_Update" FROM Salesforce."Contact" WHERE "Contact"."Id" IN (
SELECT "Contact_Merge"."ContactId_2" FROM public."Contact_Merge")

-- Update Id après mise à jour des différentes colonnes
UPDATE public."Contact_Merge_Update" SET "Id" = (SELECT "Contact_Merge"."ContactId_1" FROM public."Contact_Merge" WHERE 
	"Contact_Merge"."ContactId_2" = "Contact_Merge_Update"."Id");

-- Update Champs s360a__MergeStatus__c et s360a__MergeMessage__c
UPDATE public."Contact_Merge_Update" SET "s360a__MergeStatus__c" = 'Merge Successful (Target)';

UPDATE public."Contact_Merge_Update" SET "s360a__MergeMessage__c" = 
(SELECT '<b>Time Stamp</b>: ' || CAST (to_char(CURRENT_TIMESTAMP, 'FMDay, DD MonthYYYY HH:MI:SS AM') AS VARCHAR(50)) || '<br><b>Merged Record:</b> <a href="/' ||
"ContactId_2" || '" target="_blank">' || "ContactId_2" || '</a><br>' FROM public."Contact_Merge" WHERE "Contact_Merge"."ContactId_2" = "Contact_Merge_Update"."Id");

-- Exemple : <b>Time Stamp</b>: Thursday, 3 June 2021 10:23:41 AM<br><b>Merged Record:</b> <a href="/0033V00000FGuJtQAL" target="_blank">0033V00000FGuJtQAL</a><br>

-- Champs : "Contact"."UMR__c" à ajouter

---- Table DELETE Second Contact
SELECT "Contact"."Id" 
INTO public."Contact_Merge_Delete" FROM Salesforce."Contact" WHERE "Contact"."Id" IN (
SELECT "Contact_Merge"."ContactId_2" FROM public."Contact_Merge")

-- UPDATE Case Champs Contact 2 NULL THEN Contact 1 (exemple Birthday)
UPDATE public."Contact_Merge_Update" SET "Birthdate" = (
SELECT CASE 
	WHEN "Contact"."Birthdate" IS NULL
		THEN (SELECT "Contact"."Birthdate" FROM Salesforce."Contact" WHERE "Contact"."Id" = "Contact_Merge"."ContactId_1")
	ELSE ("Contact"."Birthdate") END
FROM Salesforce."Contact" INNER JOIN public."Contact_Merge" ON "Contact"."Id" = public."Contact_Merge"."ContactId_2"
WHERE "Contact_Merge"."ContactId_2" = "Contact_Merge_Update"."Id");

-- UPDATE LSJ
UPDATE public."Contact_Merge_Update" SET "gpi__LegacyStage__c" = (
SELECT
CASE 
	WHEN LEFT (RIGHT("Contact"."gpi__LegacyStage__c", LENGTH("Contact"."gpi__LegacyStage__c") - 4), POSITION (' ' IN RIGHT("Contact"."gpi__LegacyStage__c", LENGTH("Contact"."gpi__LegacyStage__c") - POSITION(' ' IN "Contact"."gpi__LegacyStage__c")))-1) = '1'
		THEN ("Contact"."gpi__LegacyStage__c")		
	WHEN (SELECT LEFT (RIGHT("Contact"."gpi__LegacyStage__c", LENGTH("Contact"."gpi__LegacyStage__c") - 4), POSITION (' ' IN RIGHT("Contact"."gpi__LegacyStage__c", LENGTH("Contact"."gpi__LegacyStage__c") - POSITION(' ' IN "Contact"."gpi__LegacyStage__c")))-1) FROM Salesforce."Contact" WHERE "Contact"."Id" = "Contact_Merge"."ContactId_1") = '1'
		THEN (SELECT "Contact"."gpi__LegacyStage__c" FROM Salesforce."Contact" WHERE "Contact"."Id" = "Contact_Merge"."ContactId_1")		
	WHEN "Contact"."gpi__LegacyStage__c" IS NULL
		THEN (SELECT "Contact"."gpi__LegacyStage__c" FROM Salesforce."Contact" WHERE "Contact"."Id" = "Contact_Merge"."ContactId_1")
	WHEN CAST(LEFT (RIGHT("Contact"."gpi__LegacyStage__c", LENGTH("Contact"."gpi__LegacyStage__c") - 4), POSITION (' ' IN RIGHT("Contact"."gpi__LegacyStage__c", LENGTH("Contact"."gpi__LegacyStage__c") - POSITION(' ' IN "Contact"."gpi__LegacyStage__c")))-1) AS INTEGER)
	> CAST((SELECT LEFT (RIGHT("Contact"."gpi__LegacyStage__c", LENGTH("Contact"."gpi__LegacyStage__c") - 4), POSITION (' ' IN RIGHT("Contact"."gpi__LegacyStage__c", LENGTH("Contact"."gpi__LegacyStage__c") - POSITION(' ' IN "Contact"."gpi__LegacyStage__c")))-1) FROM Salesforce."Contact" WHERE "Contact"."Id" = "Contact_Merge"."ContactId_1") AS INTEGER)
		THEN ("Contact"."gpi__LegacyStage__c")
	WHEN CAST(LEFT (RIGHT("Contact"."gpi__LegacyStage__c", LENGTH("Contact"."gpi__LegacyStage__c") - 4), POSITION (' ' IN RIGHT("Contact"."gpi__LegacyStage__c", LENGTH("Contact"."gpi__LegacyStage__c") - POSITION(' ' IN "Contact"."gpi__LegacyStage__c")))-1) AS INTEGER)
	< CAST((SELECT LEFT (RIGHT("Contact"."gpi__LegacyStage__c", LENGTH("Contact"."gpi__LegacyStage__c") - 4), POSITION (' ' IN RIGHT("Contact"."gpi__LegacyStage__c", LENGTH("Contact"."gpi__LegacyStage__c") - POSITION(' ' IN "Contact"."gpi__LegacyStage__c")))-1) FROM Salesforce."Contact" WHERE "Contact"."Id" = "Contact_Merge"."ContactId_1")  AS INTEGER)
		THEN (SELECT "Contact"."gpi__LegacyStage__c" FROM Salesforce."Contact" WHERE "Contact"."Id" = "Contact_Merge"."ContactId_1")
	ELSE (SELECT "Contact"."gpi__LegacyStage__c") END
FROM Salesforce."Contact" INNER JOIN public."Contact_Merge" ON Salesforce."Contact"."Id" = public."Contact_Merge"."ContactId_2"
WHERE "Contact_Merge"."ContactId_2" = "Contact_Merge_Update"."Id");

/*
-- REGEXP_REPLACE Numeric
SELECT "Contact"."Id", "Contact"."gpi__LegacyStage__c", REGEXP_REPLACE("Contact"."gpi__LegacyStage__c",'[^0-9]', '', 'g')
FROM Salesforce."Contact" INNER JOIN public."Contact_Merge" ON Salesforce."Contact"."Id" = public."Contact_Merge"."ContactId_1"
*/

-- UPDATE Case Contact History (exemple s360a__PhoneNumberMobilePhoneNumber__c)
UPDATE public."Contact_Merge_Update" SET "s360a__PhoneNumberMobilePhoneNumber__c" = (
SELECT
CASE
	WHEN "Contact"."s360a__PhoneNumberMobilePhoneNumber__c" IS NOT NULL AND 
	(SELECT "Contact"."s360a__PhoneNumberMobilePhoneNumber__c" FROM Salesforce."Contact" WHERE "Contact"."Id" = "Contact_Merge"."ContactId_1") IS NOT NULL AND
	((SELECT "ContactHistory"."CreatedDate" FROM Salesforce."ContactHistory" INNER JOIN public."Contact_Merge" ON "ContactHistory"."ContactId" = "Contact_Merge"."ContactId_2"
	WHERE "ContactHistory"."Field" = 's360a__PhoneNumberMobilePhoneNumber__c') > (SELECT "ContactHistory"."CreatedDate" FROM Salesforce."ContactHistory" 
	WHERE "ContactHistory"."Field" = 's360a__PhoneNumberMobilePhoneNumber__c' AND "ContactHistory"."ContactId" = "Contact_Merge"."ContactId_1"))
		THEN ("Contact"."s360a__PhoneNumberMobilePhoneNumber__c")
	WHEN "Contact"."s360a__PhoneNumberMobilePhoneNumber__c" IS NOT NULL AND (SELECT "Contact"."s360a__PhoneNumberMobilePhoneNumber__c" FROM Salesforce."Contact"
	WHERE "Contact"."Id" = "Contact_Merge"."ContactId_1") IS NOT NULL AND ((SELECT "ContactHistory"."CreatedDate" FROM Salesforce."ContactHistory" INNER JOIN 
	public."Contact_Merge" ON "ContactHistory"."ContactId" = "Contact_Merge"."ContactId_2" WHERE "ContactHistory"."Field" = 's360a__PhoneNumberMobilePhoneNumber__c') 
	< (SELECT "ContactHistory"."CreatedDate" FROM Salesforce."ContactHistory" WHERE "ContactHistory"."Field" = 's360a__PhoneNumberMobilePhoneNumber__c' AND
	"ContactHistory"."ContactId" = "Contact_Merge"."ContactId_1"))
		THEN (SELECT "Contact"."s360a__PhoneNumberMobilePhoneNumber__c" FROM Salesforce."Contact" WHERE "Contact"."Id" = "Contact_Merge"."ContactId_1") 
	WHEN "Contact"."s360a__PhoneNumberMobilePhoneNumber__c" IS NULL
		THEN (SELECT "Contact"."s360a__PhoneNumberMobilePhoneNumber__c" FROM Salesforce."Contact" WHERE "Contact"."Id" = "Contact_Merge"."ContactId_1")
	ELSE "Contact"."s360a__PhoneNumberMobilePhoneNumber__c" END
FROM Salesforce."Contact" INNER JOIN public."Contact_Merge" ON "Contact"."Id" = public."Contact_Merge"."ContactId_2"
WHERE "Contact_Merge"."ContactId_2" = "Contact_Merge_Update"."Id");

-- SELECT Case Contact History (exemple s360a__PhoneNumberHomePhoneNumber__c)
SELECT "Contact"."Id", "Contact"."s360a__PhoneNumberHomePhoneNumber__c",
CASE
	WHEN "Contact"."s360a__PhoneNumberHomePhoneNumber__c" IS NOT NULL AND 
	(SELECT "Contact"."s360a__PhoneNumberHomePhoneNumber__c" FROM Salesforce."Contact" WHERE "Contact"."Id" = "Contact_Merge"."ContactId_1") IS NOT NULL AND
	((SELECT "ContactHistory"."CreatedDate" FROM Salesforce."ContactHistory" INNER JOIN public."Contact_Merge" ON "ContactHistory"."ContactId" = "Contact_Merge"."ContactId_2"
	WHERE "ContactHistory"."Field" = 's360a__PhoneNumberHomePhoneNumber__c') > (SELECT "ContactHistory"."CreatedDate" FROM Salesforce."ContactHistory" 
	WHERE "ContactHistory"."Field" = 's360a__PhoneNumberHomePhoneNumber__c' AND "ContactHistory"."ContactId" = "Contact_Merge"."ContactId_1"))
		THEN ("Contact"."s360a__PhoneNumberHomePhoneNumber__c")
	WHEN "Contact"."s360a__PhoneNumberHomePhoneNumber__c" IS NOT NULL AND (SELECT "Contact"."s360a__PhoneNumberHomePhoneNumber__c" FROM Salesforce."Contact"
	WHERE "Contact"."Id" = "Contact_Merge"."ContactId_1") IS NOT NULL AND ((SELECT "ContactHistory"."CreatedDate" FROM Salesforce."ContactHistory" INNER JOIN 
	public."Contact_Merge" ON "ContactHistory"."ContactId" = "Contact_Merge"."ContactId_2" WHERE "ContactHistory"."Field" = 's360a__PhoneNumberHomePhoneNumber__c') 
	< (SELECT "ContactHistory"."CreatedDate" FROM Salesforce."ContactHistory" WHERE "ContactHistory"."Field" = 's360a__PhoneNumberHomePhoneNumber__c' AND
	"ContactHistory"."ContactId" = "Contact_Merge"."ContactId_1"))
		THEN (SELECT "Contact"."s360a__PhoneNumberHomePhoneNumber__c" FROM Salesforce."Contact" WHERE "Contact"."Id" = "Contact_Merge"."ContactId_1") 
	WHEN "Contact"."s360a__PhoneNumberHomePhoneNumber__c" IS NULL
		THEN (SELECT "Contact"."s360a__PhoneNumberHomePhoneNumber__c" FROM Salesforce."Contact" WHERE "Contact"."Id" = "Contact_Merge"."ContactId_1")
	ELSE "Contact"."s360a__PhoneNumberHomePhoneNumber__c" END
FROM Salesforce."Contact" INNER JOIN public."Contact_Merge" ON "Contact"."Id" = public."Contact_Merge"."ContactId_2"

-- Adresses (proposition : si Contact History L3 THEN L1 L2 L3 L4 Postcode Country)
SELECT "Contact"."Id", "Contact"."L3_ContactAddressLine3__c",
CASE
	WHEN "Contact"."L3_ContactAddressLine3__c" IS NOT NULL AND 
	(SELECT "Contact"."L3_ContactAddressLine3__c" FROM Salesforce."Contact" WHERE "Contact"."Id" = "Contact_Merge"."ContactId_1") IS NOT NULL AND
	((SELECT "ContactHistory"."CreatedDate" FROM Salesforce."ContactHistory" INNER JOIN public."Contact_Merge" ON "ContactHistory"."ContactId" = "Contact_Merge"."ContactId_2"
	WHERE "ContactHistory"."Field" = 'L3_ContactAddressLine3__c') > (SELECT "ContactHistory"."CreatedDate" FROM Salesforce."ContactHistory" 
	WHERE "ContactHistory"."Field" = 'L3_ContactAddressLine3__c' AND "ContactHistory"."ContactId" = "Contact_Merge"."ContactId_1"))
		THEN ("Contact"."L3_ContactAddressLine3__c")
	WHEN "Contact"."L3_ContactAddressLine3__c" IS NOT NULL AND (SELECT "Contact"."L3_ContactAddressLine3__c" FROM Salesforce."Contact"
	WHERE "Contact"."Id" = "Contact_Merge"."ContactId_1") IS NOT NULL AND ((SELECT "ContactHistory"."CreatedDate" FROM Salesforce."ContactHistory" INNER JOIN 
	public."Contact_Merge" ON "ContactHistory"."ContactId" = "Contact_Merge"."ContactId_2" WHERE "ContactHistory"."Field" = 'L3_ContactAddressLine3__c') 
	< (SELECT "ContactHistory"."CreatedDate" FROM Salesforce."ContactHistory" WHERE "ContactHistory"."Field" = 'L3_ContactAddressLine3__c' AND
	"ContactHistory"."ContactId" = "Contact_Merge"."ContactId_1"))
		THEN (SELECT "Contact"."L3_ContactAddressLine3__c" FROM Salesforce."Contact" WHERE "Contact"."Id" = "Contact_Merge"."ContactId_1") 
	WHEN "Contact"."L3_ContactAddressLine3__c" IS NULL
		THEN (SELECT "Contact"."L3_ContactAddressLine3__c" FROM Salesforce."Contact" WHERE "Contact"."Id" = "Contact_Merge"."ContactId_1")
	ELSE "Contact"."L3_ContactAddressLine3__c" END
FROM Salesforce."Contact" INNER JOIN public."Contact_Merge" ON "Contact"."Id" = public."Contact_Merge"."ContactId_2"

-- DR0P Tables :
-- DROP TABLE public."Staging_MergeContactId"
-- DROP TABLE public."RegularGiving_MergeContactId"
-- DROP TABLE public."Opportunity_MergeContactId"
-- DROP TABLE public."CampaignMember_MergeContact_Insert"
-- DROP TABLE public."CampaignMember_MergeContact_Delete"
-- DROP TABLE public."Case_MergeContactId"
-- DROP TABLE public."Contact_Merge_Update"
-- DROP TABLE public."Contact_Merge_Delete"

*/
END
$BODY$;
