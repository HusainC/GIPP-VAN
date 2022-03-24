def get_setup1_netezza(set_renewal_start_date, set_renewal_end_date):
    refs_sql = f"""
create table analysis_db.op.gipp_invites as
with cte as (
select dpo.skey__ as policy_key
      ,dpo.policy_number
      ,dpr.lineofbusiness_id as lob
      ,dpr.brandcode as brand
      ,dpr.nk_scheme as scheme
      ,dpo.broker_tenure
      ,sdt.date as renewal_date
      ,fsq.submissionnumber as invite_reference
      ,fsq.quote_datekey as invite_datekey
      ,fsq.quote_timekey as invite_timekey
      ,qdt.date as invite_date
      ,fsq.nk_quote_timestamp as invite_timestamp
      ,row_number() over(partition by dpo.policy_number order by fsq.nk_quote_timestamp asc) as invite_number
from edw_dm.dbo.dimpolicy dpo
inner join edw_dm.dbo.fctsorquote fsq
on dpo.skey__ = fsq.policy_key
inner join edw_dm.dbo.dimcalendardate qdt
on fsq.quote_datekey = qdt.skey__
inner join edw_dm.dbo.dimcalendardate sdt
on dpo.cover_start_date_key = sdt.skey__
inner join edw_dm.dbo.dimtransactioncontext dtc
on fsq.transactioncontext_key = dtc.skey__
inner join edw_dm.dbo.dimproduct dpr
on fsq.product_key = dpr.skey__
where
-- Set renewal date range
sdt.date >= '{set_renewal_start_date}' and sdt.date <= '{set_renewal_end_date}'
-- Set invite run window
--and fsq.nk_quote_timestamp >= '{set_renewal_start_date}' and fsq.nk_quote_timestamp <= '{set_renewal_end_date}'
-- Set LoB 'PC','CV','MC','HH'
and dpr.lineofbusiness_id in ('CV')
-- Limit to renewal invites
and dtc.nk_sourcesystem = 'PC'
and dtc.business_subtype in ('Renewal Invite (Rebroke)','Renewal Invite (Non Rebroke)')
and dtc.transactioncontexttype = 'Renewal'
)
select * from cte where invite_number = 1
;
        """

    return refs_sql


def get_setup2_netezza():
    refs_sql = f"""
create table analysis_db.op.gipp_address as
with cte as (
select i.policy_key
      ,i.policy_number
      ,i.lob
      ,i.brand
      ,i.scheme
      ,i.broker_tenure
      ,i.renewal_date
      ,i.invite_reference
      ,i.invite_datekey
      ,i.invite_timekey
      ,i.invite_date
      ,i.invite_timestamp
	  ,a.addressline1
	  ,a.addressline2
	  ,a.addressline3
	  ,a.city
	  ,a.county
	  ,a.postalcode
	  ,row_number() over(partition by i.policy_number order by a.createtime asc) as dedup
from analysis_db.op.gipp_invites i
inner join adl.sor.vw_cdc_pc_policyperiod p
on i.policy_number = p.policynumber
inner join adl.sor.vw_cdc_pc_contact c
on p.pnicontactdenorm = c.id
inner join adl.sor.vw_cdc_pc_address_hd a
on c.primaryaddressid = a.id
and a.createtime <= i.invite_timestamp
)
select * from cte where dedup = 1
;
    """
    return refs_sql


def get_setup3_netezza():
    refs_sql = f"""
create table analysis_db.op.gipp_var_base as
select i.policy_key
      ,i.policy_number
      ,i.lob
      ,i.brand
      ,i.scheme
      ,i.broker_tenure
      ,i.renewal_date
      ,i.invite_reference
      ,i.invite_datekey
      ,i.invite_timekey
      ,i.invite_date
      ,i.invite_timestamp
      ,i.addressline1
      ,i.addressline2
      ,i.addressline3
	  ,i.city
      ,i.county
      ,i.postalcode as postcode
      ,sum(case when dct.coveragegroup in ('Core','Line Item') and dct.costgroup = 'Premium' then fsq.amount else 0 end) as netpremium
      ,sum(case when dct.coveragegroup in ('Core','Line Item') and dct.costgroup = 'Commission' then fsq.amount else 0 end) as commission
      ,sum(case when dct.coveragegroup in ('Core','Line Item') and dct.costgroup = 'Tax' then fsq.amount else 0 end) as ipt
      ,sum(case when dct.costgroup = 'Fee' and dct.isincome = 1 then fsq.amount else 0 end) as fee
      ,sum(case when dct.coveragegroup = 'Ancillary' then fsq.amount else 0 end) as ancillary
      ,sum(case when dct.costgroup = 'DirectDebit' and dct.isincome = 1 then fsq.amount else 0 end) as interest
      ,sum(case when dct.coveragegroup in ('Core','Line Item') and dct.costgroup in ('Premium','Commission') then fsq.amount else 0 end) as grosspremium
      ,sum(case when dct.coveragegroup in ('Core','Line Item') and dct.costgroup in ('Premium','Commission','Tax') then fsq.amount else 0 end
          + case when dct.costgroup = 'Fee' and dct.isincome = 1 then fsq.amount else 0 end) as streetprice
      ,sum(case when dct.coveragegroup in ('Core','Line Item') and dct.costgroup in ('Premium','Commission','Tax') then fsq.amount else 0 end
          + case when dct.costgroup = 'Fee' and dct.isincome = 1 then fsq.amount else 0 end
          + case when dct.coveragegroup = 'Ancillary' then fsq.amount else 0 end) as streetprice_ancillary
      ,sum(case when dct.coveragegroup in ('Core','Line Item') and dct.costgroup in ('Premium','Commission','Tax') then fsq.amount else 0 end
          + case when dct.costgroup = 'Fee' and dct.isincome = 1 then fsq.amount else 0 end
          + case when dct.costgroup = 'DirectDebit' and dct.isincome = 1 then fsq.amount else 0 end) as streetprice_dd
      ,sum(case when dct.coveragegroup in ('Core','Line Item') and dct.costgroup in ('Premium','Commission','Tax') then fsq.amount else 0 end
          + case when dct.costgroup = 'Fee' and dct.isincome = 1 then fsq.amount else 0 end
          + case when dct.coveragegroup = 'Ancillary' then fsq.amount else 0 end
          + case when dct.costgroup = 'DirectDebit' and dct.isincome = 1 then fsq.amount else 0 end) as streetprice_ancillary_dd
from analysis_db.op.gipp_address i
inner join edw_dm.dbo.fctsorquote fsq
on i.policy_key = fsq.policy_key
and i.invite_datekey = fsq.quote_datekey
and i.invite_timekey = fsq.quote_timekey
inner join edw_dm.dbo.dimtransactioncontext dtc
on fsq.transactioncontext_key = dtc.skey__
inner join edw_dm.dbo.dimcosttype dct
on fsq.costtype_key = dct.skey__
where dtc.business_subtype in ('Renewal Invite (Rebroke)','Renewal Invite (Non Rebroke)')
and dtc.transactioncontexttype = 'Renewal'
group by i.policy_key
      ,i.policy_number
      ,i.lob
      ,i.brand
      ,i.scheme
      ,i.broker_tenure
      ,i.renewal_date
      ,i.invite_reference
      ,i.invite_datekey
      ,i.invite_timekey
      ,i.invite_date
      ,i.invite_timestamp
      ,i.addressline1
      ,i.addressline2
      ,i.addressline3
	  ,i.city
      ,i.county
      ,i.postalcode
;
"""
    return refs_sql


def get_setup3():
    refs_sql = f"""
            CREATE or replace TEMPORARY TABLE UTIL_DB.PUBLIC.hc_testing as 
            select  BRAND, renewal_date, policy_number, 
            addressline1,addressline2,addressline3, city, county, INVITE_TIMESTAMP, INVITE_REFERENCE from DEMO_DB.PUBLIC.GIPP_VAN_SUBS
            LIMIT 1000;
            """
    return refs_sql


def get_setup3_list(submission_nums="../res/submission_numbers.txt"):
    with open(submission_nums) as f:
        submission_list = f.read()

    refs_sql = f"""
            CREATE or replace TEMPORARY TABLE UTIL_DB.PUBLIC.hc_testing as 
            select  BRAND, renewal_date, policy_number, 
            addressline1,addressline2,addressline3, city, county, INVITE_TIMESTAMP, INVITE_REFERENCE from DEMO_DB.PUBLIC.GIPP_MON_SUBS
            WHERE policy_number IN ({submission_list});
            """
    return refs_sql


def get_aggids():
    refs_sql = f"""
        CREATE or replace TEMPORARY TABLE UTIL_DB.PUBLIC.hc_final as 
        with cte as(Select wkd.policy_number, r.agghub_id, r.inserttimestamp, wkd.renewal_date, r.tranname
        , wkd.brand
        ,wkd.addressline1
        ,wkd.addressline2
        ,wkd.addressline3
        ,wkd.county
        ,wkd.city
        ,wkd.INVITE_REFERENCE
        ,row_number() over(partition by wkd.INVITE_REFERENCE order by 
                            case when DATEDIFF(SECOND, r.INSERTTIMESTAMP, wkd.INVITE_TIMESTAMP) <0 then 9999999 else DATEDIFF(SECOND, r.INSERTTIMESTAMP, wkd.INVITE_TIMESTAMP) end asc,
                          CASE WHEN r.TRANNAME = 'Renewal' THEN 0 WHEN r.TRANNAME = 'QuoteDetail' THEN 1 WHEN r.TRANNAME = 'MTA' THEN 2 END) as invite_number
        from UTIL_DB.PUBLIC.hc_testing wkd

        inner join PRD_QUOTES.QUOTE_PAYLOAD.VW_POLARIS_VEH_REQ r on wkd.INVITE_REFERENCE = r.quote_reference and wkd.brand = r.INTERMEDIARY_BUSINESSSOURCETEXT
        where r.tranname not in ('TempAddVehicleAdd','TempAddDriverAdd')
        order by wkd.invite_reference, invite_number)
        Select * from cte where invite_number =1;
        """

    return refs_sql


def get_vehicle_info1():
    refs_sql = f"""
    CREATE or replace TEMPORARY TABLE UTIL_DB.PUBLIC.hc_veh_one as      
        select v.quote_reference as "QuoteReference",
        v.quote_reference,
        v.VEHICLE_MODEL as "abiCode",
        v.VEHICLE_REGNO as "registration",
        v.VEHICLE_BODYTYPE as "bodyType",
        v.VEHICLE_FIRSTREGDYEAR as "yearOfRegistration",
        v.VEHICLE_TRANSMISSIONTYPE as "transmission",
        v.VEHICLE_CUBICCAPACITY as  "engineSize",
        v.VEHICLE_MODELNAME as "model",
        v.VEHICLE_NOOFSEATS as "noOfSeats",
        v.VEHICLE_TYPEOFFUEL as "fuelType",
        v.VEHICLE_VALUE as "value",
        date(to_date(v.VEHICLE_PURCHASEDATE , 'DD/MM/YYYY'), 'YYYY-MM-DD') as "purchaseDate",
        case when v.VEHICLE_PERSONALIMPORTIND = 'N' then 'no' else 'yes' end as "importType",
        case when v.VEHICLE_LEFTORRIGHTHANDDRIVE = 'R' then 'true' else 'false' end as "rightHandDrive",
        case when v.VEHICLE_TRACKERDEVICEFITTEDIND = 'Y' then 'true' else 'false' end as "tracker",
        v.VEHICLE_POSTCODEFULL as "overnightPostCode",
        v.VEHICLE_VEHICLEKEPTDAYTIME as "parkedDaytimeData",
        v.VEHICLE_LOCATIONKEPTOVERNIGHT as "parkedOvernight",
        case
            when v.VEHICLE_OWNERSHIP = '1' then '1_PR'
            when v.VEHICLE_OWNERSHIP = '1_leased_private' then '4_LP'
            when v.VEHICLE_OWNERSHIP = '2' then '2_SP'
            when v.VEHICLE_OWNERSHIP = '3' then '3_CO'
            when v.VEHICLE_OWNERSHIP = '4' then '4_LC'
            when v.VEHICLE_OWNERSHIP = '6' then '6_FP'
            when v.VEHICLE_OWNERSHIP = '7' then '7_CL'
            when v.VEHICLE_OWNERSHIP = '8' then '8_FC'
            when v.VEHICLE_OWNERSHIP = '9' then '9_CS'
            when v.VEHICLE_OWNERSHIP = '9_society_club' then '9_OT'
            when v.VEHICLE_OWNERSHIP = 'E' then 'E_CP'
            when v.VEHICLE_OWNERSHIP = 'H' then 'H_FO'
            when v.VEHICLE_OWNERSHIP = 'H_sibling' then 'H_FS'
        end as "owner",
        case
            when v.VEHICLE_KEEPER = '1' then '1_PR'
            when v.VEHICLE_KEEPER = '1_leased_private' then '1_LP'
            when v.VEHICLE_KEEPER = '2' then '2_SP'
            when v.VEHICLE_KEEPER = '3' then '3_CO'
            when v.VEHICLE_KEEPER = '4' then '4_LC'
            when v.VEHICLE_KEEPER = '6' then '6_FP'
            when v.VEHICLE_KEEPER = '7' then '7_CL'
            when v.VEHICLE_KEEPER = '8' then '8_FC'
            when v.VEHICLE_KEEPER = '9' then '9_CS'
            when v.VEHICLE_KEEPER = '9_society_club' then '9_OT'
            when v.VEHICLE_KEEPER = 'E' then 'E_CP'
            when v.VEHICLE_KEEPER = 'H' then 'H_FO'
            when v.VEHICLE_KEEPER = 'H_sibling' then 'H_FS'
        end as "registeredKeeper",
        v.VEHICLE_ANNUALMILEAGE as "totalMileage",
        v.PROCESSINGINDICATORS_PROCESSTYPE,
         case when r.ProposerPolicyholder_InstalmentsRequestedInd = 'Y' then '3' else '1' end as "insurancePaymentType",
         r.ProposerPolicyholder_NoOfVehiclesAvailableToFamily as "noOfVehiclesHousehold",
         v.VEHICLE_GROSSWEIGHT as "grossVehicleWeight",
         case when r.POLICY_HAZARDOUSGOODSCARRIEDIND = 'Y' then 'true' else 'false' end as "isHazardousGoods",
         case when r.POLICY_SIGNEDPROPOSALIND = 'Y' then 'true' else 'false' end as "isSignWritten",
         case when v.VEHICLE_USEWITHTRAILERIND = 'Y' then 'true' else 'false' end as "towTrailer",
        fin.*
         from UTIL_DB.PUBLIC.hc_final fin
        inner join PRD_QUOTES.QUOTE_PAYLOAD.VW_POLARIS_VEH_REQ_VEHICLE v on fin.AGGHUB_ID = v.AGGHUB_ID
        inner join PRD_QUOTES.QUOTE_PAYLOAD.VW_POLARIS_VEH_REQ r on fin.AGGHUB_ID = r.AGGHUB_ID;
        """

    return refs_sql


def get_vehicle_info2():
    refs_sql = f"""
CREATE or replace TEMPORARY TABLE UTIL_DB.PUBLIC.hc_veh_two as 
        select n.NCD_CLAIMEDYEARS as "ncdGrantedYears",
        case when n.NCD_CLAIMEDPROTECTIONREQDIND = 'Y' then 'true' else 'false' end as "ncdProtect",
        n.NCD_CLAIMEDENTITLEMENTREASON as "howNcdEarn",
        n.ncd_claimedentitlementreason as "nameDriverExp",
        n.ncd_claimedyearsearned as "yearsNamedDriverExp",
        case when n.NCD_CLAIMEDCOUNTRYEARNED = 'GB' then 'true' else 'false' end as "ncdEarnedUk",
         u.USES_ABICODE as "classOfUse",
        oneg.*
         from UTIL_DB.PUBLIC.hc_veh_one oneg
        inner join PRD_QUOTES.QUOTE_PAYLOAD.VW_POLARIS_VEH_REQ_NCD n on oneg.AGGHUB_ID = n.AGGHUB_ID 
        inner join PRD_QUOTES.QUOTE_PAYLOAD.VW_POLARIS_VEH_REQ_USES u on oneg.AGGHUB_ID = u.AGGHUB_ID;
        """

    return refs_sql


def get_vehicle_info3():
    refs_sql = f"""
CREATE or replace TEMPORARY TABLE UTIL_DB.PUBLIC.hc_veh_three as 
        select s.SECURITY_MAKEMODEL as "immobiliser",
        twog.*,
        case 
            when c.COVER_CODE = '01' then 'comprehensive'
            when c.COVER_CODE = '02' then 'tpft'
            when c.COVER_CODE = '03' then 'thirdParty'
        end as "coverType"
         from UTIL_DB.PUBLIC.hc_veh_two twog
        inner join PRD_QUOTES.QUOTE_PAYLOAD.VW_POLARIS_VEH_REQ_SECURITY s on twog.AGGHUB_ID = s.AGGHUB_ID
        inner join PRD_QUOTES.QUOTE_PAYLOAD.VW_POLARIS_VEH_REQ_COVER c on twog.AGGHUB_ID = c.AGGHUB_ID;
        """

    return refs_sql


def get_vehicle_info4(cs):
    refs_sql = f"""
        select s.DRIVENBY_DRIVERNUMBER as "mainUser",
        twog.*
         from UTIL_DB.PUBLIC.hc_veh_three twog
        inner join PRD_QUOTES.QUOTE_PAYLOAD.VW_POLARIS_VEH_REQ_DRIVENBY s on twog.AGGHUB_ID = s.AGGHUB_ID
        where nvl(s.PROCESSINGINDICATORS_PROCESSTYPE, '00') != '04'
        and drivenby_drivingfrequency = 'M';
        """

    sql = get_setup3()
    cs.execute(sql)
    sql = get_aggids()
    cs.execute(sql)
    sql = get_vehicle_info1()
    cs.execute(sql)
    sql = get_vehicle_info2()
    cs.execute(sql)
    sql = get_vehicle_info3()
    cs.execute(sql)
    cs.execute(refs_sql)

    try:
        df = cs.fetch_pandas_all()
        chk = df.to_dict(orient='records')
        return chk
    finally:
        print("done")


def get_vehicle_info5():
    refs_sql = f"""
               CREATE or replace TEMPORARY TABLE UTIL_DB.PUBLIC.hc_veh_res_base as 
       with cte as( Select p.AGGHUB_ID as res_id,
                   p.inserttimestamp,
                   fin.agghub_id
        ,row_number() over(partition by fin.invite_reference order by 
                    case when DATEDIFF(SECOND, fin.INSERTTIMESTAMP, t.INSERTTIMESTAMP) <0 then 9999999 else DATEDIFF(SECOND, fin.INSERTTIMESTAMP, t.INSERTTIMESTAMP) end asc) as num
        from UTIL_DB.PUBLIC.hc_final fin
        inner join PRD_QUOTES.QUOTE_PAYLOAD.VW_POLARIS_VEH_RES_TRANRESULT t on t.quote_reference = fin.invite_reference and t.TRANNAME = fin.TRANNAME
        inner join PRD_QUOTES.QUOTE_PAYLOAD.VW_POLARIS_VEH_RES_POLDATA p on t.agghub_id = p.agghub_id and p.INTERMEDIARY_BUSINESSSOURCETEXT = fin.brand)
        Select *
        from cte 
        where num =1;
        """

    return refs_sql


def get_vehicle_info6(cs):
    refs_sql = f"""
                select c.COVER_VOLXSALLOWED, b.AGGHUB_ID from UTIL_DB.PUBLIC.hc_veh_res_base b 
        inner join PRD_QUOTES.QUOTE_PAYLOAD.VW_POLARIS_VEH_RES_COVER c on c.AGGHUB_ID = b.res_id and b.inserttimestamp = c.inserttimestamp
        where c.COVER_VEHPRN = 1;
        """

    sql = get_vehicle_info5()
    cs.execute(sql)
    sql = refs_sql
    cs.execute(sql)

    try:
        df = cs.fetch_pandas_all()
        chk = df.to_dict(orient='records')
        return chk
    finally:
        print("done")


def get_driv1():
    refs_sql = f"""
           CREATE or replace TEMPORARY TABLE UTIL_DB.PUBLIC.hc_driv_one as  
            select r.quote_reference as "QuoteReference",
           r.inserttimestamp,
           r.proposerpolicyholder_postcodefull as "postCode",
           case when r.PROPOSERPOLICYHOLDER_NOOFCHILDREN = '1' then 'true' else 'false' end as "anyChildrenUnder16",
           case when r.PROPOSERPOLICYHOLDER_HOMEOWNERIND = 'Y' then 'true' else 'false' end as "homeOwner",
           fin.agghub_id,
           r.PROCESSINGINDICATORS_PROCESSTYPE,
           r.tranname,
           fin.policy_number,
           case when fin.addressline1 is NULL then ' ' else  fin.addressline1 end as "addressLine1",
           case when fin.addressline2 is NULL then ' ' else  fin.addressline2 end  as "addressLine2",
           case when fin.addressline3 is NULL then ' ' else  fin.addressline3 end  as "addressLine3",
           case when fin.county is NULL then ' ' else  fin.county end  as "county",
           case when fin.city is NULL then ' ' else  fin.city end  as "town"
           from UTIL_DB.PUBLIC.hc_final fin
           inner join PRD_QUOTES.QUOTE_PAYLOAD.VW_POLARIS_VEH_REQ r on fin.agghub_id = r.agghub_id;
            """

    return refs_sql


def get_driv2():
    refs_sql = f"""
               CREATE or replace TEMPORARY TABLE UTIL_DB.PUBLIC.hc_driv_two as  
            select d.quote_reference,
           d.driver_title as "title",
           d.driver_forenameinitial1 as "firstName",
           d.driver_surname as "lastName",
           d.driver_prn as "driverId",
           d.DRIVER_LICENCETYPE,
           d.DRIVER_LICENCENUMBER as "number",
           date(to_date(d.driver_dateofbirth, 'DD/MM/YYYY'), 'YYYY-MM-DD') as "dateOfBirth",
           d.driver_maritalstatus as "maritalStatus",  
           date(to_date(d.driver_licencedate, 'DD/MM/YYYY'), 'YYYY-MM-DD') as "lengthHeld",
           d.driver_relationshiptoproposer as "relationship",
           case when d.Driver_EverHadPolicyCancelledInd = 'Y' then 'true' else 'false' end as "isPolicyDeclinedForDrivers",
           case when d.driver_nonmotoringconvictionind = 'Y' then 'true' else 'false' end as "nonMotoringConvictions",
           date(to_date(d.driver_ukresidencydate, 'DD/MM/YYYY'), 'YYYY-MM-DD') as "ukResident",
           case when o.OCCUPATION_FULLTIMEEMPLOYMENTIND = 'Y' then 'true' else 'false' end as "isPrimary",
           o.OCCUPATION_EMPLOYMENTTYPE as "employmentStatusCode",
           o.OCCUPATION_CODE as "employmentOccupationCode",
           o.OCCUPATION_EMPLOYERSBUSINESS as "employmentBusinessCode",
           o.PROCESSINGINDICATORS_PROCESSTYPE as "check",
           fin.*,
           d.driver_prn,
           d.date_created
        from UTIL_DB.PUBLIC.hc_driv_one fin
        inner join PRD_QUOTES.QUOTE_PAYLOAD.VW_POLARIS_VEH_REQ_DRIVER d on fin.agghub_id = d.agghub_id
        inner join PRD_QUOTES.QUOTE_PAYLOAD.VW_POLARIS_VEH_REQ_OCCUPATION o on fin.agghub_id = o.agghub_id and d.driver_prn = o.driver_prn;
            """

    return refs_sql


# def get_driv3():
#     refs_sql = f"""
# CREATE or replace TEMPORARY TABLE UTIL_DB.PUBLIC.hc_driv_check as
# select l.licencenumber, e.validto, l.licencetype, l.drivernumber, l.policynumber
# from PRD_RAW_DB.MYLICENCE_PUBLIC.VW_MYLICENCE_EXT l
# inner join PRD_RAW_DB.MYLICENCE_PUBLIC.VW_MYLICENCE_ENTITLEMENTS_EXT e on l.quoteref = e.quoteref and l.MYLICENCEID = e.MYLICENCEID and l.drivernumber = e.drivernumber
# where substring(e.entitlement_code, 1, 16) = 'Licence Group B ';
#                 """
#
#     return refs_sql


def get_driv4(cs):
    refs_sql = f"""
                       with cte as( 
       select e.driverprn,
       case when e.LICENCETYPE is NULL then e.LICENCETYPE else e.LICENCETYPE end as "type",
       --case when l.DRIVINGLICENCENUMBER is NULL then '?' else  l.DRIVINGLICENCENUMBER end as "number",
        e.MEDICALCONDITION as "medicalConditions", -- Y/N whereas a code is needed
        e.ACCESSOTHERVEHICLES as "useOtherVehicle",
        row_number() over(partition by e.quote_reference,e.driverprn order by 
            case when DATEDIFF(SECOND,  o.INSERTTIMESTAMP, e.inserttimestamp) <0 then 9999999 else DATEDIFF(SECOND,  o.INSERTTIMESTAMP, e.inserttimestamp) end desc,
            case when o.TRANNAME = 'Renewal' THEN 0 WHEN o.TRANNAME = 'QuoteDetail' THEN 1 WHEN o.TRANNAME = 'MTA' THEN 2 END) as invite_number,
        o.*
       from UTIL_DB.PUBLIC.hc_driv_two o
       inner join PRD_QUOTES.QUOTE_PAYLOAD.VW_EARNIX_REQ_DRIVER e on o.quote_reference	= e.quote_reference and e.driverprn = o.driver_prn
       -- left join PRD_QUOTES.QUOTE_PAYLOAD.VW_MYLIC_REQ l on CAST(l.quote_reference AS varchar(255)) = CAST(o.quote_reference AS varchar(255)) and driverno = e.driverprn
       )Select * from cte 
       where invite_number=1
       order by cte.agghub_id, cte.driver_prn; 
                """

    sql = get_driv1()
    cs.execute(sql)
    sql = get_driv2()
    cs.execute(sql)
    cs.execute(refs_sql)

    try:
        df = cs.fetch_pandas_all()
        chk = df.to_dict(orient='records')
        return chk
    finally:
        print("done")


def get_convictions(cs):
    refs_sql = f"""
    select
    fin.agghub_id,
    c.quote_reference,
    c.driver_prn,
    date(to_date(c.conviction_date, 'DD/MM/YYYY'), 'YYYY-MM-DD') as "date",
    c.conviction_code as "convictionCode",
    c.conviction_penaltypts as "licencePoints",
    c.conviction_suspensionperiod as "banLength"
    from UTIL_DB.PUBLIC.hc_driv_two fin
    inner join PRD_QUOTES.QUOTE_PAYLOAD.vw_polaris_veh_req_conviction c on 
    fin.agghub_id = c.agghub_id and c.inserttimestamp = fin.inserttimestamp and c.driver_prn = fin.driver_prn
    where c.conviction_code is not NULL;
                """

    sql = refs_sql
    cs.execute(sql)

    try:
        df = cs.fetch_pandas_all()
        chk = df.to_dict(orient='records')
        return chk
    finally:
        print("done")


def get_claims(cs):
    refs_sql = f"""
        select
        c.driver_prn,
        date(to_date(c.claim_date, 'DD/MM/YYYY'), 'YYYY-MM-DD') as "date",
        c.claim_claimtype as "type",
        case when c.claim_ncdlostind = 'Y' then 'true' else 'false' end as "ncdAffected",
        case when c.claim_driveratfaultind = 'Y' then 'true' else 'false' end as "fault",
        case when c.claim_coststotal is NULL then 0 else c.claim_coststotal end as "cost",
        c.quote_reference
        from  UTIL_DB.PUBLIC.hc_final fin
        inner join PRD_QUOTES.QUOTE_PAYLOAD.VW_POLARIS_VEH_REQ_CLAIM c on fin.agghub_id = c.agghub_id and c.inserttimestamp = fin.inserttimestamp;
                    """

    sql = refs_sql
    cs.execute(sql)

    try:
        df = cs.fetch_pandas_all()
        chk = df.to_dict(orient='records')
        return chk
    finally:
        print("done")


def get_modifications(cs):
    refs_sql = f"""
    select 
    m.quote_reference,
    m.agghub_id,
    m.MODIFICATIONS_CODE as "modificationAbiCode"
    from  PRD_QUOTES.QUOTE_PAYLOAD.VW_POLARIS_VEH_REQ_MODIFICATIONS m
    inner join UTIL_DB.PUBLIC.hc_final f on m.agghub_id= f.agghub_id;
                    """
    sql = refs_sql
    cs.execute(sql)

    try:
        df = cs.fetch_pandas_all()
        chk = df.to_dict(orient='records')
        return chk
    finally:
        print("done")


def get_occupations(cs):
    refs_sql = f"""
        select  
        case when o.OCCUPATION_FULLTIMEEMPLOYMENTIND = 'Y' then 'true' else 'false' end as "isPrimary",
        o.OCCUPATION_EMPLOYMENTTYPE as "employmentStatusCode",
        o.OCCUPATION_CODE as "employmentOccupationCode",
        o.OCCUPATION_EMPLOYERSBUSINESS as "employmentBusinessCode",
        o.PROCESSINGINDICATORS_PROCESSTYPE as "check",
        o.quote_reference,
        o.agghub_id,
        d.driver_prn,
        fin.policy_number
        from UTIL_DB.PUBLIC.hc_final fin
        inner join PRD_QUOTES.QUOTE_PAYLOAD.VW_POLARIS_VEH_REQ_DRIVER d on fin.agghub_id = d.agghub_id
        inner join PRD_QUOTES.QUOTE_PAYLOAD.VW_POLARIS_VEH_REQ_OCCUPATION o on fin.agghub_id = o.agghub_id and d.driver_prn = o.driver_prn;
                    """
    sql = refs_sql
    cs.execute(sql)

    try:
        df = cs.fetch_pandas_all()
        chk = df.to_dict(orient='records')
        return chk
    finally:
        print("done")
