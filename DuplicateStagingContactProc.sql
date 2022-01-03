-- PROCEDURE: public.DuplicateStagingContactProc()

-- DROP PROCEDURE public."DuplicateStagingContactProc"(); --

CREATE OR REPLACE PROCEDURE public."DuplicateStagingContactProc"(
	)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE tstart TIMESTAMP; 
DECLARE tend TIMESTAMP;
DECLARE duration INTERVAL;

BEGIN

CALL public.log_message('Update Stagings : START');
tstart := clock_timestamp();
DROP TABLE IF EXISTS public."DuplicateStagingContact";
create table 
	public."DuplicateStagingContact" as 
-- Sélection des stagings en 'Duplicate Contact'
SELECT "s360aie__Staging__c"."Id", "s360aie__Staging__c"."s360aie__Status__c", "s360aie__Staging__c"."s360aie__Processing_Contacts_Status__c","s360aie__Staging__c"."Incomplete_Comments__c",
"s360aie__Staging__c"."s360aie__ContactID__c","s360aie__Staging__c"."s360aie__Duplicate_Contact__c", "s360aie__Staging__c"."s360aie__ActionType__c",
"s360aie__Staging__c"."s360aie__Contact_First_Name__c", "s360aie__Staging__c"."s360aie__Contact_Last_Name__c",
"s360aie__Staging__c"."s360aie__Contact_Gender__c", "s360aie__Staging__c"."s360aie__Contact_Date_of_Birth2__c", "s360aie__Staging__c"."s360aie__Contact_Phone_Home__c",
"s360aie__Staging__c"."s360aie__Contact_Phone_Mobile__c", "s360aie__Staging__c"."s360aie__Contact_Email_Personal__c", "s360aie__Staging__c"."s360aie__Contact_Email_Other__c", "s360aie__Staging__c"."s360aie__Contact_Source__c"
FROM Salesforce."s360aie__Staging__c" 
WHERE "s360aie__Staging__c"."s360aie__Status__c"='Duplicate Contact' AND "s360aie__Staging__c"."s360aie__ProcessName__c" NOT IN ('EN','NewLead') 
AND "s360aie__Staging__c"."s360aie__Incomplete__c"='False' AND "s360aie__Staging__c"."s360aie__Country_of_Ownership__c"='France';

-- Update Partie Déjà PA
-- Passage du Status à 'Hold Manual'
UPDATE public."DuplicateStagingContact" SET "s360aie__Status__c"='Hold Manual' WHERE "DuplicateStagingContact"."s360aie__Status__c"='Duplicate Contact' 
AND "DuplicateStagingContact"."s360aie__ActionType__c"='New Regular Gift' AND "DuplicateStagingContact"."Incomplete_Comments__c" IS NULL
AND "s360aie__Duplicate_Contact__c" IN (SELECT "s360a__RegularGiving__c"."s360a__Contact__c" FROM salesforce."s360a__RegularGiving__c" WHERE
"s360a__RegularGiving__c"."s360a__RGStatus__c" in ('Active', 'Last Attempt Failed', 'Held','Initial'));
-- Passage de Incomplete Comments à 'Déjà PA'								
UPDATE public."DuplicateStagingContact" SET "Incomplete_Comments__c"='Déjà PA' WHERE "DuplicateStagingContact"."s360aie__Status__c"='Duplicate Contact' 
AND "DuplicateStagingContact"."s360aie__ActionType__c"='New Regular Gift' AND "DuplicateStagingContact"."Incomplete_Comments__c" IS NULL
AND "s360aie__Duplicate_Contact__c" IN (SELECT "s360a__RegularGiving__c"."s360a__Contact__c" FROM salesforce."s360a__RegularGiving__c" WHERE
"s360a__RegularGiving__c"."s360a__RGStatus__c" in ('Active', 'Last Attempt Failed', 'Held','Initial'));

-- Update Partie Contacts déjà en base
UPDATE public."DuplicateStagingContact" SET "s360aie__Contact_Gender__c" = "Contact"."s360a__Gender__c" FROM Salesforce."Contact"
WHERE "DuplicateStagingContact"."s360aie__Status__c"='Duplicate Contact' AND LOWER(TRANSLATE("Contact"."FirstName"||"Contact"."LastName"||"Contact"."s360a__EmailAddressPersonalEmailAddress__c",'çéèêëàâäùûüçôöîïÿ-','ceeeeaaauuucooiiy ')) =
LOWER(TRANSLATE("DuplicateStagingContact"."s360aie__Contact_First_Name__c"||"DuplicateStagingContact"."s360aie__Contact_Last_Name__c"||"DuplicateStagingContact"."s360aie__Contact_Email_Personal__c",'çéèêëàâäùüçôîï-','ceeeeaaauucoii '))
AND "Contact"."s360aie__IsRecruiter__c" ='False' AND "DuplicateStagingContact"."s360aie__Contact_Gender__c" IS NULL;
-- UPDATE Birthday s360aie__Contact_Date_of_Birth2__c
UPDATE public."DuplicateStagingContact" SET "s360aie__Contact_Date_of_Birth2__c" = "Contact"."Birthdate" FROM Salesforce."Contact"
WHERE "DuplicateStagingContact"."s360aie__Status__c"='Duplicate Contact' AND LOWER(TRANSLATE("Contact"."FirstName"||"Contact"."LastName"||"Contact"."s360a__EmailAddressPersonalEmailAddress__c",'çéèêëàâäùûüçôöîïÿ-','ceeeeaaauuucooiiy ')) =
LOWER(TRANSLATE("DuplicateStagingContact"."s360aie__Contact_First_Name__c"||"DuplicateStagingContact"."s360aie__Contact_Last_Name__c"||"DuplicateStagingContact"."s360aie__Contact_Email_Personal__c",'çéèêëàâäùüçôîï-','ceeeeaaauucoii '))
AND "Contact"."s360aie__IsRecruiter__c" ='False' AND "DuplicateStagingContact"."s360aie__Contact_Date_of_Birth2__c" IS NULL;
-- UPDATE Home Phone s360aie__Contact_Phone_Home__c
UPDATE public."DuplicateStagingContact" SET "s360aie__Contact_Phone_Home__c" = "Contact"."s360a__PhoneNumberHomePhoneNumber__c" FROM Salesforce."Contact"
WHERE "DuplicateStagingContact"."s360aie__Status__c"='Duplicate Contact' AND LOWER(TRANSLATE("Contact"."FirstName"||"Contact"."LastName"||"Contact"."s360a__EmailAddressPersonalEmailAddress__c",'çéèêëàâäùûüçôöîïÿ-','ceeeeaaauuucooiiy ')) =
LOWER(TRANSLATE("DuplicateStagingContact"."s360aie__Contact_First_Name__c"||"DuplicateStagingContact"."s360aie__Contact_Last_Name__c"||"DuplicateStagingContact"."s360aie__Contact_Email_Personal__c",'çéèêëàâäùüçôîï-','ceeeeaaauucoii '))
AND "Contact"."s360aie__IsRecruiter__c" ='False' AND "DuplicateStagingContact"."s360aie__Contact_Phone_Home__c" IS NULL;
-- UPDATE Phone Mobile (Mobile Phone 1) s360aie__Contact_Phone_Mobile__c
UPDATE public."DuplicateStagingContact" SET "s360aie__Contact_Phone_Mobile__c" = "Contact"."s360a__PhoneNumberMobilePhoneNumber__c" FROM Salesforce."Contact"
WHERE "DuplicateStagingContact"."s360aie__Status__c"='Duplicate Contact' AND LOWER(TRANSLATE("Contact"."FirstName"||"Contact"."LastName"||"Contact"."s360a__EmailAddressPersonalEmailAddress__c",'çéèêëàâäùûüçôöîïÿ-','ceeeeaaauuucooiiy ')) =
LOWER(TRANSLATE("DuplicateStagingContact"."s360aie__Contact_First_Name__c"||"DuplicateStagingContact"."s360aie__Contact_Last_Name__c"||"DuplicateStagingContact"."s360aie__Contact_Email_Personal__c",'çéèêëàâäùüçôîï-','ceeeeaaauucoii '))
AND "Contact"."s360aie__IsRecruiter__c" ='False' AND "DuplicateStagingContact"."s360aie__Contact_Phone_Mobile__c" IS NULL;
-- UPDATE Other Email s360aie__Contact_Email_Other__c
UPDATE public."DuplicateStagingContact" SET "s360aie__Contact_Email_Other__c" = "Contact"."s360a__EmailAddressOtherEmailAddress__c" FROM Salesforce."Contact"
WHERE "DuplicateStagingContact"."s360aie__Status__c"='Duplicate Contact' AND LOWER(TRANSLATE("Contact"."FirstName"||"Contact"."LastName"||"Contact"."s360a__EmailAddressPersonalEmailAddress__c",'çéèêëàâäùûüçôöîïÿ-','ceeeeaaauuucooiiy ')) =
LOWER(TRANSLATE("DuplicateStagingContact"."s360aie__Contact_First_Name__c"||"DuplicateStagingContact"."s360aie__Contact_Last_Name__c"||"DuplicateStagingContact"."s360aie__Contact_Email_Personal__c",'çéèêëàâäùüçôîï-','ceeeeaaauucoii '))
AND "Contact"."s360aie__IsRecruiter__c" ='False' AND "DuplicateStagingContact"."s360aie__Contact_Email_Other__c" IS NULL;
-- UPDATE Initial Source s360aie__Contact_Source__c
UPDATE public."DuplicateStagingContact" SET "s360aie__Contact_Source__c" = "Contact"."s360a__InitialContactSource__c" FROM Salesforce."Contact"
WHERE "DuplicateStagingContact"."s360aie__Status__c"='Duplicate Contact' AND LOWER(TRANSLATE("Contact"."FirstName"||"Contact"."LastName"||"Contact"."s360a__EmailAddressPersonalEmailAddress__c",'çéèêëàâäùûüçôöîïÿ-','ceeeeaaauuucooiiy ')) =
LOWER(TRANSLATE("DuplicateStagingContact"."s360aie__Contact_First_Name__c"||"DuplicateStagingContact"."s360aie__Contact_Last_Name__c"||"DuplicateStagingContact"."s360aie__Contact_Email_Personal__c",'çéèêëàâäùüçôîï-','ceeeeaaauucoii '))
AND "Contact"."s360aie__IsRecruiter__c" ='False' AND "Contact"."s360a__InitialContactSource__c" IS NOT NULL;

-- Update Partie Non Donor
-- Passage du Status à 'New'
UPDATE public."DuplicateStagingContact" SET "s360aie__Status__c"='New' WHERE "s360aie__Duplicate_Contact__c" IN
(SELECT "Contact"."Id" FROM salesforce."Contact" WHERE "Contact"."AutomaticContactCodes__c"='Non Donor') ;
-- Passage de Processing Status à 'Existing Contact'
UPDATE public."DuplicateStagingContact" SET "s360aie__Processing_Contacts_Status__c"='Existing Contact' WHERE "s360aie__Duplicate_Contact__c" IN
(SELECT "Contact"."Id" FROM salesforce."Contact" WHERE "Contact"."AutomaticContactCodes__c"='Non Donor');
-- Copie du Contact Id dans le bon champs
UPDATE public."DuplicateStagingContact" SET "s360aie__ContactID__c"="s360aie__Duplicate_Contact__c" WHERE "s360aie__Duplicate_Contact__c" IN
(SELECT "Contact"."Id" FROM salesforce."Contact" WHERE "Contact"."AutomaticContactCodes__c"='Non Donor');
UPDATE public."DuplicateStagingContact" SET "s360aie__Duplicate_Contact__c"='' WHERE "s360aie__Duplicate_Contact__c" IN
(SELECT "Contact"."Id" FROM salesforce."Contact" WHERE "Contact"."AutomaticContactCodes__c"='Non Donor');

-- DELETE ROW NOT UPDATED
DELETE FROM public."DuplicateStagingContact" WHERE "DuplicateStagingContact"."s360aie__Contact_Gender__c" IS NULL AND "DuplicateStagingContact"."s360aie__Contact_Date_of_Birth2__c" IS NULL
AND "DuplicateStagingContact"."s360aie__Contact_Phone_Home__c" IS NULL AND "DuplicateStagingContact"."s360aie__Contact_Phone_Mobile__c" IS NULL
AND "DuplicateStagingContact"."s360aie__Contact_Email_Other__c" IS NULL AND "DuplicateStagingContact"."s360aie__Contact_Source__c" ='Web' AND "DuplicateStagingContact"."s360aie__Status__c"='Duplicate Contact';

-- ALTER TABLE
ALTER TABLE public."DuplicateStagingContact" DROP COLUMN "s360aie__Contact_First_Name__c";
ALTER TABLE public."DuplicateStagingContact" DROP COLUMN "s360aie__Contact_Last_Name__c";
ALTER TABLE public."DuplicateStagingContact" DROP COLUMN "s360aie__Contact_Email_Personal__c";

-- Extraction du fichier
--SELECT * FROM public."DuplicateStagingContact";

/* ************************************************************************ */

tend := clock_timestamp();
duration := tend - tstart;
CALL public.log_message('Update Stagings : ');
CALL public.log_message('CORRECTLY FINISH.');

GRANT SELECT ON public."DuplicateStagingContact" TO PUBLIC;

END
$BODY$;
