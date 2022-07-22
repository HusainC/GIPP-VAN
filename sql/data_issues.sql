USE ROLE FG_RETAILPRICING;

USE WAREHOUSE WRK_RETAILPRICING_SMALL;
USE DATABASE WRK_RETAILPRICING;
USE SCHEMA GROWTH;

create or replace temporary table earnix_id_hus_nb as
WITH NB AS(
SELECT V.NB_SUBMISSION
      ,V.NB_DATE_CREATED AS DATE_CREATED
      ,B.nk_agg_id_quote_ref
      ,MIN(B.AGGHUB_ID) AS AGGHUB_ID_REQ_E
     ,MIN(P.AGGHUB_ID) AS AGGHUB_ID_REQ_P
     ,MIN(R.AGGHUB_ID) AS AGGHUB_ID_RES_R
FROM WRK_RETAILPRICING.GROWTH.GIPP_VAN_SUBS_JW V
LEFT JOIN "PRD_RAW_DB"."QUOTES_PUBLIC"."VW_EARNIX_VAN_REQ_BASE" B
ON  B.QUOTE_REFERENCE=V.NB_SUBMISSION
AND B.DATE_CREATED=V.NB_DATE_CREATED
LEFT JOIN "PRD_RAW_DB"."QUOTES_PUBLIC"."VW_POLARIS_VEH_REQ" P
ON  B.date_created<=P.date_created
AND B.date_created>=DATEADD('day',-1,P.date_created)
AND B.quote_reference =P.quote_reference
AND B.nk_agg_id_quote_ref=P.nk_agg_id_quote_ref
LEFT JOIN "PRD_RAW_DB"."QUOTES_PUBLIC"."VW_EARNIX_VAN_RES_POLICY" R
ON  B.date_created<=DATEADD('day',+1,R.date_created)
AND B.date_created>=R.date_created
AND B.quote_reference =R.quote_reference
AND B.nk_agg_id_quote_ref=R.nk_agg_id_quote_ref
GROUP BY NB_SUBMISSION
        ,NB_DATE_CREATED
        ,B.nk_agg_id_quote_ref)
Select * from NB;



-- Get Earnix renewal request details
-- PolicyProposer and VehicleCover
create or replace temporary table earnix_nb_base_huss as
SELECT DISTINCT
      -- P.LOAD_ID
      --,P.AGGHUB_ID
      --,P.AGGREGATOR_ID
      --,P.QUOTE_REFERENCE
      --,P.PRODUCT_CODE
      --,P.PAYLOAD_SOURCE
      --,P.INSERTTIMESTAMP
      --,P.SUBMISSIONNUMBER
      P.ACCOUNTNUMBER
      ,P.POLICYNUMBER
      ,P.ACCOUNTTYPE
      ,P.TRANSACTIONTYPE
      ,P.TRANSACTIONREASON
      ,P.ORIGINALCHANNELTYPE
      ,P.ORIGINALCHANNELCODE
      ,P.CAMPAIGNCODE
      ,CAST(P.CURRENTDATE AS DATE) AS CURRENTDATE
      ,CAST(P.INCEPTIONDATE AS DATE) AS INCEPTIONDATE
      ,CAST(P.EFFECTIVEDATE AS DATE) AS EFFECTIVEDATE
      ,CAST(P.ENDDATE AS DATE) AS ENDDATE
      ,CAST(P.FIRSTSEENDATE AS DATE) as FIRSTSEENDATE
      ,P.DAYSTOINCEPTION
      ,P.SEENTOINCEPTION
      ,DATE(D.DRIVER_DATEOFBIRTH,'DD/MM/YYYY') AS DateOfBirth
      ,P.PRICINGRANDOMID
      ,P.EMAILDOMAIN
      ,P.INSURANCEPAYMENTTYPE
      ,P.HOMEOWNERIND
      ,P.TIMEATADDRESS
      ,P.NOOFCHILDREN
      ,nvl(P.NOOFDRIVERSINFAMILY, 0) as NOOFDRIVERSINFAMILY
      ,P.NOOFVEHICLESAVAILABLETOFAMILY
      ,P.BROKERTENURE
      ,P.INSURERTENURE
      ,P.APR
      ,P.INSTALMENTS
      ,P.DEPOSIT
      ,P.EXISTINGCUSTOMER
      ,P.USERROLE
      ,P.MTACOUNT
      ,P.LASTMTADATE
      --,P.AGGHUB_QUOTE_VERSION
      --,P.HIVE_INSERT_TIMESTAMP
      ,B.*
FROM earnix_id_hus_nb B
INNER JOIN "PRD_RAW_DB"."QUOTES_PUBLIC"."VW_EARNIX_VAN_REQ_POLICY" P
ON  B.NB_SUBMISSION=P.SUBMISSIONNUMBER
AND B.DATE_CREATED=P.DATE_CREATED
AND B.AGGHUB_ID_REQ_E=P.AGGHUB_ID
INNER JOIN "PRD_RAW_DB"."QUOTES_PUBLIC"."VW_POLARIS_VEH_REQ_DRIVER" D
ON  B.NB_SUBMISSION=D.QUOTE_REFERENCE
AND B.DATE_CREATED=D.DATE_CREATED
AND B.AGGHUB_ID_REQ_P=D.AGGHUB_ID
AND nvl(D.DRIVER_PRN,1)=1
;


-- PolicyProposer and VehicleCover
create or replace temporary table earnix_nb_base_huss_fin as
SELECT DISTINCT
        --P.LOAD_ID
        --P.AGGHUB_ID
        --,P.NK_AGG_ID_QUOTE_REF
        --,P.AGGREGATOR_ID
        --,P.QUOTE_REFERENCE
        --,P.PRODUCT_CODE
        --,P.PAYLOAD_SOURCE
        --,P.INSERTTIMESTAMP
        B.*
        ,P.NEWDETAILS
        ,P.ALARMIMMOBILISER
        ,P.ANNUALMILEAGE
        ,P.BODYTYPE
        ,P.CLAIMEDENTITLEMENTREASON
        ,P.CLAIMEDPROTECTIONREQDIND
        ,P.CLAIMEDYEARS
        ,nvl(P.CLAIMEDYEARSEARNED,0) as CLAIMEDYEARSEARNED
        ,P.CLASSOFUSE
        ,P.COVERCODE
        ,P.COVERPERIOD
        ,P.CUBICCAPACITY
        ,P.DRIVERSALLOWED
        ,P.FIRSTREGDYEAR
        ,P.FUELTYPE
        ,P.KEEPER
        ,P.RIGHTHANDDRIVE
        ,P.LOCATIONKEPTOVERNIGHT
        ,P.MANUFACTURER
        ,P.MODEL
        ,P.MODIFIEDIND
        ,P.NOOFSEATS
        ,P.OWNEDMONTHS
        ,P.OWNEDYEARS
        ,P.OWNERSHIP
        ,P.PERSONALIMPORTIND
		,D.VEHICLE_POSTCODEFULL AS POSTCODEFULL
        ,P.PURCHASEDATE
        ,P.TRACKERDEVICEFITTEDIND
        ,P.TRANSMISSIONTYPE
        ,P.VALUE
        ,P.VEHICLEAGE
        ,P.VOLXSAMT
        ,P.CARRYHAZARDOUSGOODS
        ,P.INTERNALRACKINGSHELVING
        ,P.SIGNWRITTEN
        ,P.REFRIGERATED
        ,P.GROSSWEIGHT
        ,P.TOWTRAILER
        --,P.AGGHUB_QUOTE_VERSION
        --,P.HIVE_INSERT_TIMESTAMP
        --,P.DATE_CREATED
        ,nvl(P.NONSTANDARDVAN,'none') as NONSTANDARDVAN
        ,nvl(P.MOTORTRADE,'none') as MOTORTRADE
FROM earnix_nb_base_huss B
INNER JOIN "PRD_RAW_DB"."QUOTES_PUBLIC"."VW_EARNIX_VAN_REQ_VEHICLECOVER" P
ON  B.NB_SUBMISSION=P.QUOTE_REFERENCE
AND B.DATE_CREATED=P.DATE_CREATED
AND B.AGGHUB_ID_REQ_E=P.AGGHUB_ID
INNER JOIN "PRD_RAW_DB"."QUOTES_PUBLIC"."VW_POLARIS_VEH_REQ_VEHICLE" D
ON  B.NB_SUBMISSION=D.QUOTE_REFERENCE
AND B.DATE_CREATED=D.DATE_CREATED
AND B.AGGHUB_ID_REQ_P=D.AGGHUB_ID
;


create or replace temporary table earnix_nb_driver_huss as
SELECT DISTINCT
       B.*
      --,P.LOAD_ID
      --,P.XMLSHREDDINGPATH
      --,P.AGGHUB_ID
      --,P.AGGREGATOR_ID
      --,P.QUOTE_REFERENCE
      --,P.PRODUCT_CODE
      --,P.PAYLOAD_SOURCE
      --,P.INSERTTIMESTAMP
      --,P.NEWDETAILS
      ,P.DRIVERPRN
      ,P.RELATIONSHIPTOPROPOSER
      ,DATE(D.DRIVER_DATEOFBIRTH,'dd/MM/yyyy') AS DATEOFBIRTH
      ,CAST(P.UKRESIDENCYDATE AS DATE) AS UKRESIDENCYDATE
      ,P.ACCESSOTHERVEHICLES
      ,P.LICENCETYPE
      ,P.PASSPLUSIND
      ,P.LICENCEYEARS
      ,P.LICENCEMONTHS
	  ,D.DRIVER_MARITALSTATUS AS MARITALSTATUS
	  ,D.DRIVER_MEDICALCONDITIONIND AS MEDICALCONDITION
      ,P.MYLICENCEIND
      ,P.MYLICENCERESULT
      ,D.DRIVER_NONMOTORINGCONVICTIONIND AS NONMOTORCONVICTIONS
      ,P.INSURANCEREFUSED
      ,P.ANYDRIVERPOLICY
      ,P.PUBLICLIABILITY
      ,P.MEMBERTRADEORGANISATION
      ,P.PRIMARYEMPLOYMENTTYPE as employmenttype_full
      ,P.PRIMARYEMPLOYERSBUSINESS as employersbusiness_full
      ,P.PRIMARYOCCUPATIONCODE as occupationcode_full
      ,P.COMPANYESTABLISHED
      ,nvl(P.OTHEREMPLOYMENTTYPE,'none') as employmenttype_part
      ,nvl(P.OTHEREMPLOYERSBUSINESS,'none') as employersbusiness_part
      ,nvl(P.OTHEROCCUPATIONCODE,'none') as occupationcode_part
      --,P.AGGHUB_QUOTE_VERSION
      --,P.HIVE_INSERT_TIMESTAMP
FROM earnix_id_hus_nb B
INNER JOIN "PRD_RAW_DB"."QUOTES_PUBLIC"."VW_EARNIX_VAN_REQ_DRIVER" P
ON  B.NB_SUBMISSION=P.QUOTE_REFERENCE
AND B.DATE_CREATED=P.DATE_CREATED
AND B.AGGHUB_ID_REQ_E=P.AGGHUB_ID
INNER JOIN "PRD_RAW_DB"."QUOTES_PUBLIC"."VW_POLARIS_VEH_REQ_DRIVER" D
ON  B.NB_SUBMISSION=D.QUOTE_REFERENCE
AND B.DATE_CREATED=D.DATE_CREATED
AND B.AGGHUB_ID_REQ_P=D.AGGHUB_ID
and D.driver_prn = P.driverprn
;


create or replace temporary table earnix_nb_claim_huss as
SELECT DISTINCT
       B.*
      --,P.LOAD_ID
      --,P.XMLSHREDDINGPATH
      --,P.AGGHUB_ID
      --,P.AGGREGATOR_ID
      --,P.QUOTE_REFERENCE
      --,P.PRODUCT_CODE
      --,P.PAYLOAD_SOURCE
      --,P.INSERTTIMESTAMP
      ,P.NEWDETAILS
      ,P.DRIVERPRN
      ,CAST(P.CLAIMDATE AS DATE) AS CLAIMDATE
      ,P.CLAIMTYPE
      ,P.CLAIMCOST
      ,P.CLAIMSETTLED
      ,P.DRIVERATFAULT
      ,P.NCDAFFECTED
      --,P.AGGHUB_QUOTE_VERSION
      --,P.HIVE_INSERT_TIMESTAMP
FROM earnix_id_hus_nb B
INNER JOIN "PRD_RAW_DB"."QUOTES_PUBLIC"."VW_EARNIX_VAN_REQ_CLAIM" P
ON  B.NB_SUBMISSION=P.QUOTE_REFERENCE
AND B.DATE_CREATED=P.DATE_CREATED
AND B.AGGHUB_ID_REQ_E=P.AGGHUB_ID
;




create or replace temporary table earnix_nb_convic_huss as
SELECT DISTINCT
       B.*
      --,P.LOAD_ID
      --,P.XMLSHREDDINGPATH
      --,P.AGGHUB_ID
      --,P.AGGREGATOR_ID
      --,P.QUOTE_REFERENCE
      --,P.PRODUCT_CODE
      --,P.PAYLOAD_SOURCE
      --,P.INSERTTIMESTAMP
      ,P.MYLICENCERESULT
      ,P.DRIVER_DRIVERPRN
      ,P.NEWDETAILS
      ,P.DRIVERPRN
      ,CAST(P.CONVICTIONDATE AS DATE) AS CONVICTIONDATE
      ,P.CONVICTIONCODE
      --,P.AGGHUB_QUOTE_VERSION
      --,P.HIVE_INSERT_TIMESTAMP
FROM earnix_id_hus_nb B
INNER JOIN "PRD_RAW_DB"."QUOTES_PUBLIC"."VW_EARNIX_VAN_REQ_CONVICTION" P
ON  B.NB_SUBMISSION=P.QUOTE_REFERENCE
AND B.DATE_CREATED=P.DATE_CREATED
AND B.AGGHUB_ID_REQ_E=P.AGGHUB_ID
;

--RN

create or replace temporary table earnix_id_hus_rn as
WITH RN AS(
SELECT V.RN_SUBMISSION
      ,V.RN_DATE_CREATED AS DATE_CREATED
      ,B.nk_agg_id_quote_ref
      ,MIN(B.AGGHUB_ID) AS AGGHUB_ID_REQ_E
      ,MIN(P.AGGHUB_ID) AS AGGHUB_ID_REQ_P
      ,MIN(R.AGGHUB_ID) AS AGGHUB_ID_RES_R
FROM WRK_RETAILPRICING.GROWTH.GIPP_VAN_SUBS_JW V
LEFT JOIN "PRD_RAW_DB"."QUOTES_PUBLIC"."VW_EARNIX_VAN_REQ_BASE" B
ON  B.QUOTE_REFERENCE=V.RN_SUBMISSION
AND B.DATE_CREATED=V.RN_DATE_CREATED
LEFT JOIN "PRD_RAW_DB"."QUOTES_PUBLIC"."VW_POLARIS_VEH_REQ" P
ON  B.date_created<=P.date_created
AND B.date_created>=DATEADD('day',-1,P.date_created)
AND B.quote_reference =P.quote_reference
AND B.nk_agg_id_quote_ref   =P.nk_agg_id_quote_ref
LEFT JOIN "PRD_RAW_DB"."QUOTES_PUBLIC"."VW_EARNIX_VAN_RES_POLICY" R
ON  B.date_created<=DATEADD('day',+1,R.date_created)
AND B.date_created>=R.date_created
AND B.quote_reference =R.quote_reference
AND B.nk_agg_id_quote_ref=R.nk_agg_id_quote_ref
GROUP BY RN_SUBMISSION
        ,RN_DATE_CREATED
        ,B.nk_agg_id_quote_ref
)
SELECT * FROM RN;


-- Get Earnix renewal request details
-- PolicyProposer and VehicleCover
create or replace temporary table earnix_rn_base_huss as
SELECT DISTINCT
      --P.LOAD_ID
      --,P.AGGHUB_ID
      --,P.AGGREGATOR_ID
      --,P.QUOTE_REFERENCE
      --,P.PRODUCT_CODE
      --,P.PAYLOAD_SOURCE
      --,P.INSERTTIMESTAMP
      --,P.SUBMISSIONNUMBER
      P.ACCOUNTNUMBER
      ,P.POLICYNUMBER
      ,P.ACCOUNTTYPE
      ,P.TRANSACTIONTYPE
      ,P.TRANSACTIONREASON
      ,P.ORIGINALCHANNELTYPE
      ,P.ORIGINALCHANNELCODE
      ,P.CAMPAIGNCODE
      ,CAST(P.CURRENTDATE AS DATE) AS CURRENTDATE
      ,CAST(P.INCEPTIONDATE AS DATE) AS INCEPTIONDATE
      ,CAST(P.EFFECTIVEDATE AS DATE) AS EFFECTIVEDATE
      ,CAST(P.ENDDATE AS DATE) AS ENDDATE
      ,CAST(P.FIRSTSEENDATE AS DATE) AS FIRSTSEENDATE
      ,P.DAYSTOINCEPTION
      ,P.SEENTOINCEPTION
      ,DATE(D.DRIVER_DATEOFBIRTH,'DD/MM/YYYY') AS DateOfBirth
      ,P.PRICINGRANDOMID
      ,P.EMAILDOMAIN
      ,P.INSURANCEPAYMENTTYPE
      ,P.HOMEOWNERIND
      ,P.TIMEATADDRESS
      ,P.NOOFCHILDREN
      ,P.NOOFDRIVERSINFAMILY
      ,P.NOOFVEHICLESAVAILABLETOFAMILY
      ,P.BROKERTENURE
      ,P.INSURERTENURE
      ,P.APR
      ,P.INSTALMENTS
      ,P.DEPOSIT
      ,P.EXISTINGCUSTOMER
      ,P.USERROLE
      ,P.MTACOUNT
      ,P.LASTMTADATE
      --,P.AGGHUB_QUOTE_VERSION
      --,P.HIVE_INSERT_TIMESTAMP
      ,B.*
FROM earnix_id_hus_rn B
INNER JOIN "PRD_RAW_DB"."QUOTES_PUBLIC"."VW_EARNIX_VAN_REQ_POLICY" P
ON  B.RN_SUBMISSION=P.SUBMISSIONNUMBER
AND B.DATE_CREATED=P.DATE_CREATED
AND B.AGGHUB_ID_REQ_E=P.AGGHUB_ID
INNER JOIN "PRD_RAW_DB"."QUOTES_PUBLIC"."VW_POLARIS_VEH_REQ_DRIVER" D
ON  B.RN_SUBMISSION=D.QUOTE_REFERENCE
AND B.DATE_CREATED=D.DATE_CREATED
AND B.AGGHUB_ID_REQ_P=D.AGGHUB_ID
AND nvl(D.DRIVER_PRN,1)=1
;


-- PolicyProposer and VehicleCover
create or replace temporary table earnix_rn_base_huss_fin as
SELECT DISTINCT
        --P.LOAD_ID
        --,P.AGGHUB_ID
       -- ,P.NK_AGG_ID_QUOTE_REF
        --,P.AGGREGATOR_ID
        --,P.QUOTE_REFERENCE
        --,P.PRODUCT_CODE
        --,P.PAYLOAD_SOURCE
        --,P.INSERTTIMESTAMP
        P.NEWDETAILS
        ,P.ALARMIMMOBILISER
        ,P.ANNUALMILEAGE
        ,P.BODYTYPE
        ,P.CLAIMEDENTITLEMENTREASON
        ,P.CLAIMEDPROTECTIONREQDIND
        ,P.CLAIMEDYEARS
        ,nvl(P.CLAIMEDYEARSEARNED,0) as CLAIMEDYEARSEARNED
        ,P.CLASSOFUSE
        ,P.COVERCODE
        ,P.COVERPERIOD
        ,P.CUBICCAPACITY
        ,P.DRIVERSALLOWED
        ,P.FIRSTREGDYEAR
        ,P.FUELTYPE
        ,P.KEEPER
        ,P.RIGHTHANDDRIVE
        ,P.LOCATIONKEPTOVERNIGHT
        ,P.MANUFACTURER
        ,P.MODEL
        ,P.MODIFIEDIND
        ,P.NOOFSEATS
        ,P.OWNEDMONTHS
        ,P.OWNEDYEARS
        ,P.OWNERSHIP
        ,P.PERSONALIMPORTIND
		,D.VEHICLE_POSTCODEFULL AS POSTCODEFULL
        ,P.PURCHASEDATE
        ,P.TRACKERDEVICEFITTEDIND
        ,P.TRANSMISSIONTYPE
        ,P.VALUE
        ,P.VEHICLEAGE
        ,P.VOLXSAMT
        ,P.CARRYHAZARDOUSGOODS
        ,P.INTERNALRACKINGSHELVING
        ,P.SIGNWRITTEN
        ,P.REFRIGERATED
        ,P.GROSSWEIGHT
        ,P.TOWTRAILER
        --,P.AGGHUB_QUOTE_VERSION
        --,P.HIVE_INSERT_TIMESTAMP
        --,P.DATE_CREATED
        ,nvl(P.NONSTANDARDVAN,'none') as NONSTANDARDVAN
        ,P.MOTORTRADE
        ,b.*
FROM earnix_rn_base_huss B
INNER JOIN "PRD_RAW_DB"."QUOTES_PUBLIC"."VW_EARNIX_VAN_REQ_VEHICLECOVER" P
ON  B.RN_SUBMISSION=P.QUOTE_REFERENCE
AND B.DATE_CREATED=P.DATE_CREATED
AND B.AGGHUB_ID_REQ_E=P.AGGHUB_ID
INNER JOIN "PRD_RAW_DB"."QUOTES_PUBLIC"."VW_POLARIS_VEH_REQ_VEHICLE" D
ON  B.RN_SUBMISSION=D.QUOTE_REFERENCE
AND B.DATE_CREATED=D.DATE_CREATED
AND B.AGGHUB_ID_REQ_P=D.AGGHUB_ID
;


create or replace temporary table earnix_rn_driver_huss as
SELECT DISTINCT
       B.*
      --,P.LOAD_ID
      --,P.XMLSHREDDINGPATH
      --,P.AGGHUB_ID
      --,P.AGGREGATOR_ID
      --,P.QUOTE_REFERENCE
      --,P.PRODUCT_CODE
      --,P.PAYLOAD_SOURCE
      --,P.INSERTTIMESTAMP
      ,P.NEWDETAILS
      ,P.DRIVERPRN
      ,P.RELATIONSHIPTOPROPOSER
      ,DATE(D.DRIVER_DATEOFBIRTH,'dd/MM/yyyy') AS DATEOFBIRTH
      ,CAST(P.UKRESIDENCYDATE AS DATE) AS UKRESIDENCYDATE
      ,P.ACCESSOTHERVEHICLES
      ,P.LICENCETYPE
      ,P.PASSPLUSIND
      ,P.LICENCEYEARS
      ,P.LICENCEMONTHS
	  ,D.DRIVER_MARITALSTATUS AS MARITALSTATUS
	  ,D.DRIVER_MEDICALCONDITIONIND AS MEDICALCONDITION
      ,P.MYLICENCEIND
      ,P.MYLICENCERESULT
      ,D.DRIVER_NONMOTORINGCONVICTIONIND AS NONMOTORCONVICTIONS
      ,P.INSURANCEREFUSED
      ,P.ANYDRIVERPOLICY
      ,P.PUBLICLIABILITY
      ,P.MEMBERTRADEORGANISATION
      ,P.PRIMARYEMPLOYMENTTYPE as employmenttype_full
      ,P.PRIMARYEMPLOYERSBUSINESS as employersbusiness_full
      ,P.PRIMARYOCCUPATIONCODE as occupationcode_full
      ,P.COMPANYESTABLISHED
      ,nvl(P.OTHEREMPLOYMENTTYPE,'none') as employmenttype_part
      ,nvl(P.OTHEREMPLOYERSBUSINESS,'none') as employersbusiness_part
      ,nvl(P.OTHEROCCUPATIONCODE,'none') as occupationcode_part
      --,P.AGGHUB_QUOTE_VERSION
      --,P.HIVE_INSERT_TIMESTAMP
FROM earnix_id_hus_rn B
INNER JOIN "PRD_RAW_DB"."QUOTES_PUBLIC"."VW_EARNIX_VAN_REQ_DRIVER" P
ON  B.RN_SUBMISSION=P.QUOTE_REFERENCE
AND B.DATE_CREATED=P.DATE_CREATED
AND B.AGGHUB_ID_REQ_E=P.AGGHUB_ID
INNER JOIN "PRD_RAW_DB"."QUOTES_PUBLIC"."VW_POLARIS_VEH_REQ_DRIVER" D
ON  B.RN_SUBMISSION=D.QUOTE_REFERENCE
AND B.DATE_CREATED=D.DATE_CREATED
AND B.AGGHUB_ID_REQ_P=D.AGGHUB_ID
and D.driver_prn = P.driverprn
;


create or replace temporary table earnix_rn_claim_huss as
SELECT DISTINCT
       B.*
      --,P.LOAD_ID
      --,P.XMLSHREDDINGPATH
      --,P.AGGHUB_ID
      --,P.AGGREGATOR_ID
      --,P.QUOTE_REFERENCE
      --,P.PRODUCT_CODE
      --,P.PAYLOAD_SOURCE
      --,P.INSERTTIMESTAMP
      ,P.NEWDETAILS
      ,P.DRIVERPRN
      ,CAST(P.CLAIMDATE AS DATE) AS CLAIMDATE
      ,P.CLAIMTYPE
      ,P.CLAIMCOST
      ,P.CLAIMSETTLED
      ,P.DRIVERATFAULT
      ,P.NCDAFFECTED
      --,P.AGGHUB_QUOTE_VERSION
      --,P.HIVE_INSERT_TIMESTAMP
FROM earnix_id_hus_rn B
INNER JOIN "PRD_RAW_DB"."QUOTES_PUBLIC"."VW_EARNIX_VAN_REQ_CLAIM" P
ON  B.RN_SUBMISSION=P.QUOTE_REFERENCE
AND B.DATE_CREATED=P.DATE_CREATED
AND B.AGGHUB_ID_REQ_E=P.AGGHUB_ID
;




create or replace temporary table earnix_rn_convic_huss as
SELECT DISTINCT
       B.*
      --,P.LOAD_ID
      --,P.XMLSHREDDINGPATH
      --,P.AGGHUB_ID
      --,P.AGGREGATOR_ID
      --,P.QUOTE_REFERENCE
      --,P.PRODUCT_CODE
      --,P.PAYLOAD_SOURCE
      --,P.INSERTTIMESTAMP
      ,P.MYLICENCERESULT
      ,P.DRIVER_DRIVERPRN
      ,P.NEWDETAILS
      ,P.DRIVERPRN
      ,CAST(P.CONVICTIONDATE AS DATE) AS CONVICTIONDATE
      ,P.CONVICTIONCODE
      --,P.AGGHUB_QUOTE_VERSION
      --,P.HIVE_INSERT_TIMESTAMP
FROM earnix_id_hus_rn B
INNER JOIN "PRD_RAW_DB"."QUOTES_PUBLIC"."VW_EARNIX_VAN_REQ_CONVICTION" P
ON  B.RN_SUBMISSION=P.QUOTE_REFERENCE
AND B.DATE_CREATED=P.DATE_CREATED
AND B.AGGHUB_ID_REQ_E=P.AGGHUB_ID
;


--RN END

select * from WRK_RETAILPRICING.GROWTH.GIPP_VAN_SUBS_JW g;

create or replace temporary table check_base_van as
Select g.rn_submission
,g.nb_submission
,g.rn_date_created
,g.nb_date_created as nb_date_created
      --,case when P.AGGHUB_ID = v.AGGHUB_ID then 0 else 1 end as AGGHUB_ID
      --,case when P.NK_AGG_ID_QUOTE_REF = v.NK_AGG_ID_QUOTE_REF then 0 else 1 end as NK_AGG_ID_QUOTE_REF
      --,case when P.AGGREGATOR_ID = v.AGGREGATOR_ID  then 0 else 1 end as AGGREGATOR_ID
      --,case when P.QUOTE_REFERENCE = v.QUOTE_REFERENCE  then 0 else 1 end as QUOTE_REFERENCE
      --,case when P.PRODUCT_CODE = v.PRODUCT_CODE then 0 else 1 end as PRODUCT_CODE
      --,case when P.PAYLOAD_SOURCE = v.PAYLOAD_SOURCE then 0 else 1 end as PAYLOAD_SOURCE
      --,case when P.INSERTTIMESTAMP = v.INSERTTIMESTAMP then 0 else 1 end as INSERTTIMESTAMP
      --,case when P.SUBMISSIONNUMBER = v.SUBMISSIONNUMBER  then 0 else 1 end as SUBMISSIONNUMBER
      --,case when P.ACCOUNTNUMBER = v.ACCOUNTNUMBER then 0 else 1 end as ACCOUNTNUMBER
      --,case when P.POLICYNUMBER = v.POLICYNUMBER then 0 else 1 end as POLICYNUMBER
      --,case when P.ACCOUNTTYPE = v.ACCOUNTTYPE then 0 else 1 end as ACCOUNTTYPE
      --,case when P.TRANSACTIONTYPE = v.TRANSACTIONTYPE then 0 else 1 end as TRANSACTIONTYPE
      --,case when P.TRANSACTIONREASON = v.TRANSACTIONREASON then 0 else 1 end as TRANSACTIONREASON
      ,case when P.ORIGINALCHANNELTYPE = v.ORIGINALCHANNELTYPE then 0 else 1 end as ORIGINALCHANNELTYPE
      ,case when P.ORIGINALCHANNELCODE = v.ORIGINALCHANNELCODE then 0 else 1 end as ORIGINALCHANNELCODE
      --,case when P.CAMPAIGNCODE = v.CAMPAIGNCODE then 0 else 1 end as CAMPAIGNCODE
      --,case when P.CURRENTDATE = v.CURRENTDATE then 0 else 1 end as CURRENTDATE
      ,0 as currentdatetime
      --,case when P.INCEPTIONDATE = v.INCEPTIONDATE then 0 else 1 end as INCEPTIONDATE
      ,case when P.EFFECTIVEDATE = v.EFFECTIVEDATE  then 0 else 1 end as EFFECTIVEDATE
      --,case when P.ENDDATE = v.ENDDATE  then 0 else 1 end as ENDDATE
      --,case when P.FIRSTSEENDATE = v.FIRSTSEENDATE  then 0 else 1 end as FIRSTSEENDATE
      --,case when P.DAYSTOINCEPTION = v.DAYSTOINCEPTION then 0 else 1 end as DAYSTOINCEPTION
      --,case when P.SEENTOINCEPTION = v.SEENTOINCEPTION then 0 else 1 end as SEENTOINCEPTION
      ,case when P.DateOfBirth = v.DateOfBirth  then 0 else 1 end as DATEOFBIRTH
      ,case when P.PRICINGRANDOMID = v.PRICINGRANDOMID then 0 else 1 end as PRICINGRANDOMID
      --,case when P.EMAILDOMAIN = v.EMAILDOMAIN  then 0 else 1 end as EMAILDOMAIN
      ,case when P.INSURANCEPAYMENTTYPE = v.INSURANCEPAYMENTTYPE then 0 else 1 end as INSURANCEPAYMENTTYPE
      ,case when P.HOMEOWNERIND = v.HOMEOWNERIND  then 0 else 1 end as HOMEOWNERIND
      ,case when nvl(P.TIMEATADDRESS,0) = nvl(v.TIMEATADDRESS,0) then 0 else 1 end as TIMEATADDRESS
      ,case when P.NOOFCHILDREN = v.NOOFCHILDREN then 0 else 1 end as NOOFCHILDREN
      ,case when P.NOOFDRIVERSINFAMILY = v.NOOFDRIVERSINFAMILY     then 0 else 1 end as NOOFDRIVERSINFAMILY
      ,case when P.NOOFVEHICLESAVAILABLETOFAMILY = v.NOOFVEHICLESAVAILABLETOFAMILY  then 0 else 1 end as NOOFVEHICLESAVAILABLETOFAMILY
      --,case when P.BROKERTENURE = v.BROKERTENURE  then 0 else 1 end as BROKERTENURE
      --,case when P.INSURERTENURE = v.INSURERTENURE then 0 else 1 end as INSURERTENURE
      --,case when P.APR = v.APR  then 0 else 1 end as APR
      ,case when P.INSTALMENTS = v.INSTALMENTS  then 0 else 1 end as INSTALMENTS
      ,case when P.DEPOSIT = v.DEPOSIT  then 0 else 1 end as DEPOSIT
      --,case when P.EXISTINGCUSTOMER = v.EXISTINGCUSTOMER  then 0 else 1 end as EXISTINGCUSTOMER
      --,case when P.USERROLE = v.USERROLE then 0 else 1 end as USERROLE
      --,case when P.MTACOUNT = v.MTACOUNT then 0 else 1 end as MTACOUNT
      --,case when P.LASTMTADATE = v.LASTMTADATE then 0 else 1 end as LASTMTADATE
      --,case when P.LOAD_ID = v.LOAD_ID then 0 else 1 end as LOAD_ID
	  --,case when P.AGGHUB_ID = v.AGGHUB_ID then 0 else 1 end as AGGHUB_ID
	  --,case when P.NK_AGG_ID_QUOTE_REF = v.NK_AGG_ID_QUOTE_REF then 0 else 1 end as NK_AGG_ID_QUOTE_REF
	  --,case when P.AGGREGATOR_ID = v.AGGREGATOR_ID then 0 else 1 end as AGGREGATOR_ID
	  --,case when P.QUOTE_REFERENCE = v.QUOTE_REFERENCE then 0 else 1 end as QUOTE_REFERENCE
	  ,case when P.ALARMIMMOBILISER = v.ALARMIMMOBILISER then 0 else 1 end as ALARMIMMOBILISER
	  ,case when P.ANNUALMILEAGE = v.ANNUALMILEAGE then 0 else 1 end as ANNUALMILEAGE
	  ,case when P.BODYTYPE = v.BODYTYPE then 0 else 1 end as BODYTYPE
	  ,case when P.CLAIMEDENTITLEMENTREASON = v.CLAIMEDENTITLEMENTREASON then 0 else 1 end as CLAIMEDENTITLEMENTREASON
	  ,case when P.CLAIMEDPROTECTIONREQDIND = v.CLAIMEDPROTECTIONREQDIND then 0 else 1 end as CLAIMEDPROTECTIONREQDIND
	  ,case when P.CLAIMEDYEARS = v.CLAIMEDYEARS then 0 else 1 end as CLAIMEDYEARS
	  ,case when P.CLAIMEDYEARSEARNED = v.CLAIMEDYEARSEARNED then 0 else 1 end as CLAIMEDYEARSEARNED
	  ,case when P.CLASSOFUSE = v.CLASSOFUSE then 0 else 1 end as CLASSOFUSE
	  ,case when P.COVERCODE = v.COVERCODE then 0 else 1 end as COVERCODE
	  ,case when P.COVERPERIOD = v.COVERPERIOD then 0 else 1 end as COVERPERIOD
	  ,case when P.CUBICCAPACITY = v.CUBICCAPACITY then 0 else 1 end as CUBICCAPACITY
	  ,case when P.DRIVERSALLOWED = v.DRIVERSALLOWED then 0 else 1 end as DRIVERSALLOWED
	  ,case when P.FIRSTREGDYEAR = v.FIRSTREGDYEAR then 0 else 1 end as FIRSTREGDYEAR
	  ,case when nvl(P.FUELTYPE,0) = nvl(v.FUELTYPE,0) then 0 else 1 end as FUELTYPE
	  ,case when P.KEEPER = v.KEEPER then 0 else 1 end as KEEPER
	  ,case when P.RIGHTHANDDRIVE = v.RIGHTHANDDRIVE then 0 else 1 end as RIGHTHANDDRIVE
	  ,case when P.LOCATIONKEPTOVERNIGHT = v.LOCATIONKEPTOVERNIGHT then 0 else 1 end as LOCATIONKEPTOVERNIGHT
	  ,0 as MANUFACTURER
	  ,case when P.MODEL = v.MODEL then 0 else 1 end as MODEL
	  ,case when P.MODIFIEDIND = v.MODIFIEDIND then 0 else 1 end as MODIFIEDIND
	  ,case when P.NOOFSEATS = v.NOOFSEATS then 0 else 1 end as NOOFSEATS
	  ,case when P.OWNEDMONTHS = v.OWNEDMONTHS then 0 else 1 end as OWNEDMONTHS
	  ,case when P.OWNEDYEARS = v.OWNEDYEARS then 0 else 1 end as OWNEDYEARS
	  ,case when P.OWNERSHIP = v.OWNERSHIP then 0 else 1 end as OWNERSHIP
	  ,case when P.PERSONALIMPORTIND = v.PERSONALIMPORTIND then 0 else 1 end as PERSONALIMPORTIND
	  ,case when P.POSTCODEFULL = v.POSTCODEFULL then 0 else 1 end as POSTCODEFULL
      ,case when P.PURCHASEDATE = v.PURCHASEDATE then 0 else 1 end as PURCHASEDATE
      ,case when P.TRACKERDEVICEFITTEDIND = v.TRACKERDEVICEFITTEDIND then 0 else 1 end as TRACKERDEVICEFITTEDIND
      ,case when P.TRANSMISSIONTYPE = v.TRANSMISSIONTYPE then 0 else 1 end as TRANSMISSIONTYPE
      ,case when P.VALUE = v.VALUE then 0 else 1 end as VALUE
      ,case when P.VEHICLEAGE = v.VEHICLEAGE then 0 else 1 end as VEHICLEAGE
      ,case when P.VOLXSAMT = v.VOLXSAMT then 0 else 1 end as VOLXSAMT
      ,case when P.CARRYHAZARDOUSGOODS = v.CARRYHAZARDOUSGOODS then 0 else 1 end as CARRYHAZARDOUSGOODS
      ,case when P.INTERNALRACKINGSHELVING = v.INTERNALRACKINGSHELVING then 0 else 1 end as INTERNALRACKINGSHELVING
      ,case when P.SIGNWRITTEN = v.SIGNWRITTEN then 0 else 1 end as SIGNWRITTEN
      ,case when P.REFRIGERATED = v.REFRIGERATED then 0 else 1 end as REFRIGERATED
      ,case when P.GROSSWEIGHT = v.GROSSWEIGHT then 0 else 1 end as GROSSWEIGHT
      ,case when P.TOWTRAILER = v.TOWTRAILER then 0 else 1 end as TOWTRAILER
      --,case when P.AGGHUB_QUOTE_VERSION = v.AGGHUB_QUOTE_VERSION then 0 else 1 end as AGGHUB_QUOTE_VERSION
      --,case when P.HIVE_INSERT_TIMESTAMP = v.HIVE_INSERT_TIMESTAMP then 0 else 1 end as HIVE_INSERT_TIMESTAMP
      --,case when P.DATE_CREATED = v.DATE_CREATED then 0 else 1 end as DATE_CREATED
      ,case when P.NONSTANDARDVAN = v.NONSTANDARDVAN then 0 else 1 end as NONSTANDARDVAN
      --,case when P.MOTORTRADE = v.MOTORTRADE then 0 else 1 end as MOTORTRADE
from WRK_RETAILPRICING.GROWTH.GIPP_VAN_SUBS_JW g
inner join earnix_nb_base_huss_fin p
on g.nb_submission = p.nb_submission
inner join earnix_rn_base_huss_fin v
on g.rn_submission = v.rn_submission
;

create or replace temporary table check_driver_van as
select
       --case when P.LOAD_ID	= v.LOAD_ID then 0 else 1 end as LOAD_ID
      --,case when P.XMLSHREDDINGPATH	= v.XMLSHREDDINGPATH then 0 else 1 end as XMLSHREDDINGPATH
      --,case when P.AGGHUB_ID = v.AGGHUB_ID then 0 else 1 end as AGGHUB_ID
      --,case when P.NK_AGG_ID_QUOTE_REF = v.NK_AGG_ID_QUOTE_REF then 0 else 1 end as NK_AGG_ID_QUOTE_REF
      --,case when P.AGGREGATOR_ID = v.AGGREGATOR_ID then 0 else 1 end as AGGREGATOR_ID
      --,case when P.QUOTE_REFERENCE = v.QUOTE_REFERENCE then 0 else 1 end as QUOTE_REFERENCE
     -- ,case when P.PRODUCT_CODE = v.PRODUCT_CODE then 0 else 1 end as PRODUCT_CODE
      --,case when P.PAYLOAD_SOURCE = v.PAYLOAD_SOURCE then 0 else 1 end as PAYLOAD_SOURCE
      --,case when P.INSERTTIMESTAMP = v.INSERTTIMESTAMP then 0 else 1 end as INSERTTIMESTAMP
      --case when P.NEWDETAILS = v.NEWDETAILS then 0 else 1 end as NEWDETAILS
      case when P.DRIVERPRN = v.DRIVERPRN then 0 else 1 end as DRIVERPRN
      ,case when P.RELATIONSHIPTOPROPOSER  = v.RELATIONSHIPTOPROPOSER then 0 else 1 end as RELATIONSHIPTOPROPOSER
      ,case when P.DATEOFBIRTH = v.DATEOFBIRTH then 0 else 1 end as DATEOFBIRTH
      ,case when P.UKRESIDENCYDATE = v.UKRESIDENCYDATE then 0 else 1 end as UKRESIDENCYDATE
      ,case when P.ACCESSOTHERVEHICLES = v.ACCESSOTHERVEHICLES then 0 else 1 end as ACCESSOTHERVEHICLES
      ,case when P.LICENCETYPE = v.LICENCETYPE then 0 else 1 end as LICENCETYPE
      ,case when P.PASSPLUSIND = v.PASSPLUSIND then 0 else 1 end as PASSPLUSIND
      ,case when P.LICENCEYEARS = v.LICENCEYEARS then 0 else 1 end as LICENCEYEARS
      ,case when P.LICENCEMONTHS = v.LICENCEMONTHS then 0 else 1 end as LICENCEMONTHS
	  ,case when P.MARITALSTATUS = v.MARITALSTATUS then 0 else 1 end as MARITALSTATUS
	  ,case when P.MEDICALCONDITION = v.MEDICALCONDITION then 0 else 1 end as MEDICALCONDITION
      ,case when P.MYLICENCEIND = v.MYLICENCEIND then 0 else 1 end as MYLICENCEIND
      ,case when nvl(P.MYLICENCERESULT,0) = nvl(v.MYLICENCERESULT,0) then 0 else 1 end as MYLICENCERESULT
      ,case when P.NONMOTORCONVICTIONS = v.NONMOTORCONVICTIONS then 0 else 1 end as NONMOTORCONVICTIONS
      ,case when P.INSURANCEREFUSED = v.INSURANCEREFUSED then 0 else 1 end as INSURANCEREFUSED
      ,case when P.ANYDRIVERPOLICY = v.ANYDRIVERPOLICY then 0 else 1 end as ANYDRIVERPOLICY
      ,case when P.PUBLICLIABILITY = v.PUBLICLIABILITY then 0 else 1 end as PUBLICLIABILITY
      ,case when P.MEMBERTRADEORGANISATION  = v.MEMBERTRADEORGANISATION then 0 else 1 end as MEMBERTRADEORGANISATION
      ,case when p.employersbusiness_full = v.employersbusiness_full then 0 else 1 end as employersbusiness_full
      ,case when p.occupationcode_full = v.occupationcode_full then 0 else 1 end as occupationcode_full
      ,case when p.employmenttype_full = v.employmenttype_full then 0 else 1 end as employmenttype_full
      ,case when P.COMPANYESTABLISHED = v.COMPANYESTABLISHED then 0 else 1 end as COMPANYESTABLISHED
      ,case when p.employersbusiness_part = v.employersbusiness_part then 0 else 1 end as employersbusiness_part
      ,case when p.occupationcode_part = v.occupationcode_part then 0 else 1 end as occupationcode_part
      ,case when p.employmenttype_part = v.employmenttype_part then 0 else 1 end as employmenttype_part
      --,case when P.AGGHUB_QUOTE_VERSION = v.AGGHUB_QUOTE_VERSION then 0 else 1 end as AGGHUB_QUOTE_VERSION
      ,g.nb_submission
      ,g.rn_submission
from WRK_RETAILPRICING.GROWTH.GIPP_VAN_SUBS_JW g
inner join earnix_nb_driver_huss p
on g.nb_submission = p.nb_submission
inner join earnix_rn_driver_huss v
on g.rn_submission = v.rn_submission
and p.driverprn = v.driverprn
;



create or replace temporary table check_conviction_van as
select g.*
,r.driverprn
,case when r.CONVICTIONCODE = n.CONVICTIONCODE then 0 else 1 end as code
,case when r.CONVICTIONDATE = n.CONVICTIONDATE then 0 else 1 end as date
from WRK_RETAILPRICING.GROWTH.GIPP_VAN_SUBS_JW g
inner join earnix_rn_convic_huss r
on g.rn_submission = r.rn_submission
left join earnix_nb_convic_huss n
on g.nb_submission = n.nb_submission
and r.driverprn = n.driverprn
;

create or replace temporary table check_claim_van as
select g.*
,r.driverprn
,r.CLAIMTYPE
,r.CLAIMDATE
,r.CLAIMCOST
,r.DRIVERATFAULT
,n.CLAIMCOST as cost_nb
,case when n.CLAIMTYPE is not null then 0 else 1 end as claim_match
,case when n.CLAIMTYPE is not null and floor(cast(nvl(r.CLAIMCOST,'0') as numeric(10,2))) = floor(cast(nvl(n.CLAIMCOST,'0') as numeric(10,2))) then 0 else 1 end as cost_match
from wrk_retailpricing.car.gipp_mon_subs_MH_testing g
inner join earnix_rn_claim_huss r
on g.rn_submission = r.rn_submission
left join earnix_nb_claim_huss n
on g.nb_submission = n.nb_submission
and r.driverprn = n.driverprn
and r.CLAIMTYPE = n.CLAIMTYPE
and r.CLAIMDATE = n.CLAIMDATE
and r.DRIVERATFAULT = n.DRIVERATFAULT
;




-- Summarise
create or replace temporary table check_summary as
with base_cte as (
select rn_submission
      ,nb_submission
      ,rn_date_created
      ,nb_date_created
      ,sum(pricingrandomid) as date_issue
      ,sum(originalchanneltype+originalchannelcode) as channel_issue
      ,sum(dateofbirth+postcodefull
           +homeownerind+noofchildren+noofvehiclesavailabletofamily) as proposer_issue
      --,sum(propertymatchpolicy+propertystringpolicy+propertymatchrisk+propertystringrisk) as propdb_issue
      ,sum(covercode+coverperiod+volxsamt+classofuse+driversallowed+annualmileage) as cover_issue
      ,sum(ownership+keeper+locationkeptovernight
           +purchasedate+firstregdyear+value+manufacturer+model+bodytype+noofseats
           +cubiccapacity+fueltype+transmissiontype+modifiedind
           +personalimportind+alarmimmobiliser+trackerdevicefittedind) as vehicle_issue
      ,sum(claimedyears+claimedentitlementreason+claimedyearsearned+claimedprotectionreqdind) as ncd_issue
      --,sum(creditscore+idscore) as credit_issue
      --,sum(hpivfs+hpihri+hpikeepers) as hpi_issue
from check_base_van
group by rn_submission, nb_submission, rn_date_created, nb_date_created
),

driver_rn_cnt as (
select rn_submission
      ,count(driverprn) as driver_count
from earnix_rn_driver_huss
group by rn_submission
),

driver_nb_cnt as (
select nb_submission
      ,count(driverprn) as driver_count
from earnix_nb_driver_huss
group by nb_submission
),

driver_cte as (
select rn_submission
      ,nb_submission
      ,sum(dateofbirth+maritalstatus+ukresidencydate+nonmotorconvictions+accessothervehicles
           +medicalcondition+relationshiptoproposer) as driver_issue
      ,sum(licenceyears+licencemonths+licencetype+mylicenceind+mylicenceresult+passplusind) as licence_issue
      ,sum(employersbusiness_full+occupationcode_full+employmenttype_full) as full_emp_issue
      ,sum(employersbusiness_part+occupationcode_part+employmenttype_part) as part_emp_issue
from check_driver_van
group by rn_submission, nb_submission
),

claim_rn_cnt as (
select rn_submission
      ,count(*) as claim_count
from earnix_rn_claim_huss
group by rn_submission
),

claim_nb_cnt as (
select nb_submission
      ,count(*) as claim_count
from earnix_nb_claim_huss
group by nb_submission
),

conv_rn_cnt as (
select rn_submission
      ,count(*) as conv_count
from earnix_rn_convic_huss
group by rn_submission
),

conv_nb_cnt as (
select nb_submission
      ,count(*) as conv_count
from earnix_nb_convic_huss
group by nb_submission
),

claim_cte as (
select rn_submission
      ,nb_submission
      ,sum(claim_match) as nb_claim_missing
      ,sum(cost_match) as nb_cost_wrong
from check_claim_van
group by rn_submission, nb_submission
),

conv_cte as (
select rn_submission
      ,nb_submission
      ,sum(code) as code_wrong
      ,sum(date) as date_wrong
from check_conviction_van
group by rn_submission, nb_submission
)

--eci_cte as (
--select rn_submission
--      ,nb_submission
--      ,sum(daystoinception) as dti_issue
--      ,sum(cuepiscore+cuescore) as cue_issue
--      ,sum(paymenttype+instalmentsrequestedind) as payment_issue
--from check_eci
--group by rn_submission, nb_submission
--)

select b.*
      --,e.dti_issue
      --,e.cue_issue
      --,e.payment_issue
      ,case when dr.driver_count != dn.driver_count then 1 else 0 end as driver_num_issue
      ,d.driver_issue
      ,d.licence_issue
      --,d.full_emp_issue
      ,d.part_emp_issue
      ,case when nvl(clr.claim_count,0) != nvl(cln.claim_count,0) then 1 else 0 end as claim_num_issue
      ,cl.nb_claim_missing as nb_claim_missing
      ,cl.nb_cost_wrong as nb_claim_wrong
      ,case when nvl(cnr.conv_count,0) != nvl(cnn.conv_count,0) then 1 else 0 end as conv_num_issue
      ,cn.code_wrong as conv_code_wrong
      ,cn.date_wrong as conv_date_wrong
from base_cte b
left join driver_rn_cnt dr
on b.rn_submission = dr.rn_submission
left join driver_nb_cnt dn
on b.nb_submission = dn.nb_submission
left join driver_cte d
on b.rn_submission = d.rn_submission
left join claim_rn_cnt clr
on b.rn_submission = clr.rn_submission
left join claim_nb_cnt cln
on b.nb_submission = cln.nb_submission
left join claim_cte cl
on b.rn_submission = cl.rn_submission
left join conv_rn_cnt cnr
on b.rn_submission = cnr.rn_submission
left join conv_nb_cnt cnn
on b.nb_submission = cnn.nb_submission
left join conv_cte cn
on b.rn_submission = cn.rn_submission
--left join eci_cte e
--on b.nb_submission = e.nb_submission
;


--FULL VERSIONS


-- Fuller version to aid debugging
create or replace temporary table check_summary_full as
with driver_rn_cnt as (
select rn_submission
      ,count(driverprn) as driver_count
from earnix_rn_driver_huss
group by rn_submission
),

driver_nb_cnt as (
select nb_submission
      ,count(driverprn) as driver_count
from earnix_nb_driver_huss
group by nb_submission
),


driver_cte as (
select rn_submission
      ,nb_submission
      ,sum(dateofbirth) as driver_dateofbirth
      --,sum(age)as driver_age
      ,sum(maritalstatus) as driver_maritalstatus
      ,sum(ukresidencydate) as ukresidencydate
      --,sum(ukresidentyears) as ukresidentyears
      --,sum(ukresidentfrombirth) as ukresidentfrombirth
      ,sum(nonmotorconvictions) as nonmotorconvictions
      ,sum(accessothervehicles) as accessothervehicles
      ,sum(medicalcondition) as medicalcondition
      ,sum(relationshiptoproposer) as relationshiptoproposer
      --,sum(drivesvehicle) as drivesvehicle
      ,sum(licenceyears) as licenceyears
      ,sum(licencemonths) as licencemonths
      ,sum(licencetype) as licencetype
      ,sum(mylicenceind) as mylicenceind
      ,sum(mylicenceresult) as mylicenceresult
      ,sum(passplusind) as passplusind
      ,sum(employersbusiness_full) as employersbusiness_full
      ,sum(occupationcode_full) as occupationcode_full
      ,sum(employmenttype_full) as employmenttype_full
      ,sum(employersbusiness_part) as employersbusiness_part
      ,sum(occupationcode_part) as occupationcode_part
      ,sum(employmenttype_part) as employmenttype_part
from check_driver_van
group by rn_submission, nb_submission
),

claim_rn_cnt as (
select rn_submission
      ,count(*) as claim_count
from earnix_rn_claim_huss
group by rn_submission
),

claim_nb_cnt as (
select nb_submission
      ,count(*) as claim_count
from earnix_nb_claim_huss
group by nb_submission
),

conv_rn_cnt as (
select rn_submission
      ,count(*) as conv_count
from earnix_rn_convic_huss
group by rn_submission
),

conv_nb_cnt as (
select nb_submission
      ,count(*) as conv_count
from earnix_nb_convic_huss
group by nb_submission
),

claim_cte as (
select rn_submission
      ,nb_submission
      ,sum(claim_match) as nb_claim_missing
      ,sum(cost_match) as nb_cost_wrong
from check_claim_van
group by rn_submission, nb_submission
),

conv_cte as (
select rn_submission
      ,nb_submission
      ,sum(code) as code_wrong
      ,sum(date) as date_wrong
from check_conviction_van
group by rn_submission, nb_submission
)

--eci_cte as (
--select rn_submission
--      ,nb_submission
--      ,sum(daystoinception) as dti_issue
--      ,sum(cuepiscore+cuescore) as cue_issue
--      ,sum(paymenttype+instalmentsrequestedind) as payment_issue
--from check_eci
--group by rn_submission, nb_submission
--)

select b.*
      --,e.dti_issue
      --,e.cue_issue
      --,e.payment_issue
      ,case when dr.driver_count != dn.driver_count then 1 else 0 end as driver_num_issue
      ,d.driver_dateofbirth
      --,d.driver_age
      ,d.driver_maritalstatus
      ,d.ukresidencydate
      --,d.ukresidentyears
      --,d.ukresidentfrombirth
      ,d.nonmotorconvictions
      ,d.accessothervehicles
      ,d.medicalcondition
      ,d.relationshiptoproposer
      --,d.drivesvehicle
      ,d.licenceyears
      ,d.licencemonths
      ,d.licencetype
      ,d.mylicenceind
      ,d.mylicenceresult
      ,d.passplusind
      ,d.employersbusiness_full
      ,d.occupationcode_full
      ,d.employmenttype_full
      ,d.employersbusiness_part
      ,d.occupationcode_part
      ,d.employmenttype_part
      ,case when nvl(clr.claim_count,0) != nvl(cln.claim_count,0) then 1 else 0 end as claim_num_issue
      ,cl.nb_claim_missing
      ,cl.nb_cost_wrong as nb_claim_wrong
      ,case when nvl(cnr.conv_count,0) != nvl(cnn.conv_count,0) then 1 else 0 end as conv_num_issue
      ,cn.code_wrong as conv_code_wrong
      ,cn.date_wrong as conv_date_wrong
from check_base_van b
left join driver_rn_cnt dr
on b.rn_submission = dr.rn_submission
left join driver_nb_cnt dn
on b.nb_submission = dn.nb_submission
left join driver_cte d
on b.rn_submission = d.rn_submission
left join claim_rn_cnt clr
on b.rn_submission = clr.rn_submission
left join claim_nb_cnt cln
on b.nb_submission = cln.nb_submission
left join claim_cte cl
on b.rn_submission = cl.rn_submission
left join conv_rn_cnt cnr
on b.rn_submission = cnr.rn_submission
left join conv_nb_cnt cnn
on b.nb_submission = cnn.nb_submission
left join conv_cte cn
on b.rn_submission = cn.rn_submission
--left join eci_cte e
--on b.nb_submission = e.nb_submission
;

-- Data issues tab
select * from check_summary_full
order by rn_submission asc;