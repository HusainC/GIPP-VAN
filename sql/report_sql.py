import pandas as pd
from snowflake.connector.pandas_tools import write_pandas


def get_results_file(cs):
    sql = "USE ROLE FG_RETAILPRICING"
    cs.execute(sql)
    sql = "use warehouse wrk_retailpricing_medium;"
    cs.execute(sql)
    sql = "use database wrk_retailpricing;"
    cs.execute(sql)
    sql = "use schema car;"
    cs.execute(sql)
    sql = "ALTER SESSION SET USE_CACHED_RESULT = FALSE;"
    cs.execute(sql)
    auto_zero = """
create or replace temporary table gipp_dates as
with rnl as (
select g.rn_submission
      ,b.date_created as rn_date_created
      ,row_number() over(partition by g.rn_submission
                        order by b.date_created asc) as date_no
from wrk_retailpricing.car.gipp_van_subs g
left join PRD_RAW_DB.QUOTES_PUBLIC.vw_earnix_req_base b
on g.rn_submission = b.quote_reference and b.date_created >= '2021-12-01'
),
nb as (
select g.nb_submission
      ,b.date_created as nb_date_created
      ,row_number() over(partition by g.nb_submission
                        order by b.date_created asc) as date_no
from wrk_retailpricing.car.gipp_van_subs g
left join PRD_RAW_DB.QUOTES_PUBLIC.vw_earnix_req_base b
on g.nb_submission = b.quote_reference and b.date_created >= '2021-12-01'
)
select g.rn_submission
      ,g.nb_submission
      ,rnl.rn_date_created
      ,nb.nb_date_created
from wrk_retailpricing.car.gipp_van_subs g
left join rnl on g.rn_submission = rnl.rn_submission and rnl.date_no = 1
left join nb on g.nb_submission = nb.nb_submission and nb.date_no = 1
;
    """
    auto_one = """
create or replace table wrk_retailpricing.car.gipp_van_subs as
select * from gipp_dates
where rn_submission is not null and nb_submission is not null
and rn_date_created is not null and nb_date_created is not null;
    """
    auto_two = """
create or replace temporary table earnix_id as
select g.rn_submission
      ,g.rn_date_created
      ,q.agghub_id as request_id
      ,q.agghub_quote_version as rn_agghub_quote_version
      ,cast(r.agghub_id as int) as response_id
      ,row_number() over(partition by g.rn_submission
                        order by cast(q.inserttimestamp as timestamp) asc, r.agghub_id asc) as earnix_no
from wrk_retailpricing.car.gipp_van_subs g
left join PRD_RAW_DB.QUOTES_PUBLIC.vw_earnix_req_scheme q
on g.rn_submission = q.quote_reference
and g.rn_date_created = q.date_created
left join PRD_RAW_DB.QUOTES_PUBLIC.vw_earnix_res_scheme r
on q.quote_reference = r.quote_reference
and q.scheme = r.scheme
and q.brand = r.brand
and q.isincumbent = r.isincumbent
and q.agghub_id < r.agghub_id
and q.agghub_quote_version = r.agghub_quote_version
where g.rn_submission is not null
and q.payload_source != 'SOR-Renewal'
;
    """
    auto_three = """
   create or replace temporary table earnix_rn as
select distinct e.rn_submission
      ,e.rn_date_created
      ,e.rn_agghub_quote_version
      ,q.scheme
      ,q.brand
      ,v.claimedprotectionreqdind as pncd
      ,p.instalmentsrequestedind as dd_duq
      ,case when cast(p.pricingrandomid as smallint) in (0,1,2,3,4,5,18,21,69,70)
        and cast(p.effectivedate as date) >= '2022-01-22'
        then 'CommissionModel' else 'Optimisation' end as rates
      ,cast(q.netpremium as numeric(10,2)) as netpremium
      ,cast(q.netpremiumpncd as numeric(10,2)) as netpremiumpncd
      ,q.referral
      ,q.isincumbent
      ,q.ischeapest
      ,cast(r.commission as numeric(10,2)) as commission
      ,cast(r.commissionpncd as numeric(10,2)) as commissionpncd
      ,case when substring(q.scheme,1,2) = 'CO' and (cast(p.apr as float) > 0 or (p.apr is null and p.instalmentsrequestedind = 'Y')) then cast(q.netpremium as numeric(10,2)) * 0.025
            when substring(q.scheme,1,2) = 'CO' and (cast(p.apr as float) = 0 or (p.apr is null and p.instalmentsrequestedind = 'N')) then cast(q.netpremium as numeric(10,2)) * -0.05
            when substring(q.scheme,1,2) in ('AD','AP','YD') and p.accounttype = 'Staff' then cast(q.netpremium as numeric(10,2)) * -0.25
            else 0 end as sirncdamount
      ,case when substring(q.scheme,1,2) = 'CO' and (cast(p.apr as float) > 0 or (p.apr is null and p.instalmentsrequestedind = 'Y')) then cast(q.netpremiumpncd as numeric(10,2)) * 0.025
            when substring(q.scheme,1,2) = 'CO' and (cast(p.apr as float) = 0 or (p.apr is null and p.instalmentsrequestedind = 'N')) then cast(q.netpremiumpncd as numeric(10,2)) * -0.05
            when substring(q.scheme,1,2) in ('AD','AP','YD') and p.accounttype = 'Staff' then cast(q.netpremiumpncd as numeric(10,2)) * -0.25
            else 0 end as sirpncdamount
      ,r.isselected
      ,q.lastannualpremiumgross
      ,q.lastannualpremiumnet
      ,mod(datediff(day, cast('1903-04-09' as date), p.dateofbirth), 311) as pricingrandomid_311
from earnix_id e
left join PRD_RAW_DB.QUOTES_PUBLIC.vw_earnix_req_scheme q
on e.request_id = q.agghub_id
and e.rn_date_created = q.date_created
left join PRD_RAW_DB.QUOTES_PUBLIC.vw_earnix_req_policyproposer p
on q.agghub_id = p.agghub_id
and q.date_created = p.date_created
left join PRD_RAW_DB.QUOTES_PUBLIC.vw_earnix_req_vehiclecover v
on q.agghub_id = v.agghub_id
and q.date_created = v.date_created
left join PRD_RAW_DB.QUOTES_PUBLIC.vw_earnix_res_scheme r
on e.response_id = r.agghub_id
and e.rn_date_created = r.date_created
and q.scheme = r.scheme
and q.brand = r.brand
and q.isincumbent = r.isincumbent
and q.agghub_quote_version = r.agghub_quote_version
where e.earnix_no = 1
;

    """
    auto_four = """
create or replace temporary table earnix_id as
select g.nb_submission
      ,g.nb_date_created
      ,q.agghub_id as request_id
      ,q.agghub_quote_version as nb_agghub_quote_version
      ,r.agghub_id as response_id
      ,row_number() over(partition by g.nb_submission
                        order by cast(q.inserttimestamp as timestamp) asc, r.agghub_id asc) as earnix_no
from wrk_retailpricing.car.gipp_van_subs g
left join PRD_RAW_DB.QUOTES_PUBLIC.vw_earnix_req_scheme q
on g.nb_submission = q.quote_reference
and g.nb_date_created = q.date_created
left join PRD_RAW_DB.QUOTES_PUBLIC.vw_earnix_res_scheme r
on q.quote_reference = r.quote_reference
and q.scheme = r.scheme
and q.brand = r.brand
and q.agghub_id < r.agghub_id
and q.agghub_quote_version = r.agghub_quote_version
where g.nb_submission is not null
;
    """
    auto_five = """
    -- Get Earnix new business net premiums, SIRs (calculation for now), commissions and other useful bits
create or replace temporary table earnix_nb as
with eci as (
select e.nb_submission
      ,d.value as invitedpayment
      ,dti.value as daystoinception
from earnix_id e
left join PRD_RAW_DB.QUOTES_PUBLIC.vw_earnix_req_dataenrichment d
on e.request_id = d.agghub_id
and e.nb_date_created = d.date_created
and d.type = 'ECI'
and d.key = 'InvitedPayment'
--where e.earnix_no = 1
left join PRD_RAW_DB.QUOTES_PUBLIC.vw_earnix_req_dataenrichment dti
on e.request_id = dti.agghub_id
and e.nb_date_created = dti.date_created
and dti.type = 'ECI'
and dti.key = 'DTI'
where e.earnix_no = 1
)

select distinct e.nb_submission
      ,e.nb_date_created
      ,e.nb_agghub_quote_version
      ,q.scheme
      ,q.brand
      ,v.claimedprotectionreqdind as pncd
      ,p.instalmentsrequestedind as dd_duq
      ,p.daystoinception as pol_dti
      ,d.daystoinception as eci_dti
      ,case when cast(p.pricingrandomid as smallint) in (0,1,2,3,4,5,18,21,69,70)
        and cast(p.effectivedate as date) >= '2021-12-27'
        then 'CommissionModel' else 'Optimisation' end as rates
      ,cast(q.netpremium as numeric(10,2)) as netpremium
      ,cast(q.netpremiumpncd as numeric(10,2)) as netpremiumpncd
      ,q.referral
      ,cast(r.commission as numeric(10,2)) as commission
      ,cast(r.commissionpncd as numeric(10,2)) as commissionpncd
      ,case when substring(q.scheme,1,2) = 'CO' and (d.invitedpayment = 'true' or (d.invitedpayment is null and p.instalmentsrequestedind = 'Y')) then cast(q.netpremium as numeric(10,2)) * 0.025
            when substring(q.scheme,1,2) = 'CO' and (d.invitedpayment != 'true' or (d.invitedpayment is null and p.instalmentsrequestedind = 'N')) then cast(q.netpremium as numeric(10,2)) * -0.05
            when substring(q.scheme,1,2) in ('AD','AP','YD') and p.accounttype = 'Staff' then cast(q.netpremium as numeric(10,2)) * -0.25
            else 0 end as sirncdamount
      ,case when substring(q.scheme,1,2) = 'CO' and (d.invitedpayment = 'true' or (d.invitedpayment is null and p.instalmentsrequestedind = 'Y')) then cast(q.netpremiumpncd as numeric(10,2)) * 0.025
            when substring(q.scheme,1,2) = 'CO' and (d.invitedpayment != 'true' or (d.invitedpayment is null and p.instalmentsrequestedind = 'N')) then cast(q.netpremiumpncd as numeric(10,2)) * -0.05
            when substring(q.scheme,1,2) in ('AD','AP','YD') and p.accounttype = 'Staff' then cast(q.netpremiumpncd as numeric(10,2)) * -0.25
            else 0 end as sirpncdamount
from earnix_id e
left join PRD_RAW_DB.QUOTES_PUBLIC.vw_earnix_req_scheme q
on e.request_id = q.agghub_id
and e.nb_date_created = q.date_created
left join PRD_RAW_DB.QUOTES_PUBLIC.vw_earnix_req_policyproposer p
on e.request_id = p.agghub_id
and e.nb_date_created = p.date_created
left join PRD_RAW_DB.QUOTES_PUBLIC.vw_earnix_req_vehiclecover v
on e.request_id = v.agghub_id
and e.nb_date_created = v.date_created
left join PRD_RAW_DB.QUOTES_PUBLIC.vw_earnix_res_scheme r
on e.response_id = r.agghub_id
and e.nb_date_created = r.date_created
and q.scheme = r.scheme
and q.brand = r.brand
and q.agghub_quote_version = r.agghub_quote_version
left join eci d
on e.nb_submission = d.nb_submission
where e.earnix_no = 1
;
    """
    auto_six = """
    -- Join and summarise new business and renewal Earnix data
create or replace temporary table gipp_mon as

with brand as (
-- Identify invited brand
select g.nb_submission
      ,g.rn_submission
      ,g.nb_date_created
      ,g.rn_date_created
      ,e.rn_agghub_quote_version
      ,e.brand
      ,e.pncd
      ,e.dd_duq
      ,e.rates
from wrk_retailpricing.car.gipp_van_subs g
left join earnix_rn e
on g.rn_submission = e.rn_submission
and (e.isincumbent = 'true' or e.ischeapest = 'true')
where g.nb_submission is not null
),

nb as (
-- Get cheapest new business non-referred scheme for the invited brand
select b.*
      ,n.nb_agghub_quote_version
      ,n.rates as nb_rates
      ,n.scheme
      ,n.netpremium
      ,n.netpremiumpncd
      ,n.commission
      ,n.commissionpncd
      ,n.sirncdamount
      ,n.sirpncdamount
      ,n.pol_dti
      ,n.eci_dti
      ,row_number() over(partition by b.nb_submission
                         order by
                          case when n.referral = 'Y' then 1 else 0 end asc
                         ,case when b.pncd = 'Y'
                               then n.netpremiumpncd + n.sirpncdamount + case when n.commissionpncd <= 0 then n.commissionpncd - 20
                                                                              when n.commissionpncd > 0 and n.commissionpncd <= 20/1.12 then n.commissionpncd * 1.12 - 20
                                                                              else n.commissionpncd - 20/1.12 end
                               else n.netpremium + n.sirncdamount + case when n.commission <= 0 then n.commission - 20
                                                                         when n.commission > 0 and n.commission <= 20/1.12 then n.commission * 1.12 - 20
                                                                         else n.commission - 20/1.12 end
                          end asc
                        ) as quote_no
from brand b
left join earnix_nb n
on b.nb_submission = n.nb_submission
and b.brand = n.brand
),

enbp as (
-- Get cheapest auto-rebroke non-referred scheme for the invited brand
select b.*
      ,n.scheme
      ,n.netpremium
      ,n.netpremiumpncd
      ,n.commission
      ,n.commissionpncd
      ,n.sirncdamount
      ,n.sirpncdamount
//      ,n.covea_net
//      ,n.covea_net_pncd
      ,row_number() over(partition by b.nb_submission
                         order by
                          case when n.referral = 'Y' then 1 else 0 end asc
                         ,case when b.pncd = 'Y'
                               then n.netpremiumpncd + n.sirpncdamount + case when n.commissionpncd <= 0 then n.commissionpncd - 20
                                                                              when n.commissionpncd > 0 and n.commissionpncd <= 20/1.12 then n.commissionpncd * 1.12 - 20
                                                                              else n.commissionpncd - 20/1.12 end
                               else n.netpremium + n.sirncdamount + case when n.commission <= 0 then n.commission - 20
                                                                         when n.commission > 0 and n.commission <= 20/1.12 then n.commission * 1.12 - 20
                                                                         else n.commission - 20/1.12 end
                          end asc
                        ) as quote_no
from brand b
left join earnix_rn n
on b.rn_submission = n.rn_submission
and b.brand = n.brand
and n.isincumbent != 'true'
)

-- Summarise
select g.*
      ,new.rn_agghub_quote_version
      ,new.nb_agghub_quote_version
      ,new.pncd
      ,new.dd_duq
      ,new.rates
      ,new.nb_rates
      ,case when ren.isincumbent = 'true' then 'Y' else 'N' end as incumbent_present
      ,case when inv.isincumbent = 'true' then 'Y' else 'N' end as invited_incumbent
      ,ren.lastannualpremiumgross * (1.35 + 
            case 
                when ren.pricingrandomid_311 in (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,20,21,23,24,25,26,28,32,33,35,41,46,48,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,188,189,190) then -0.05
                when ren.pricingrandomid_311 in (107,108,122,123,126,128,131,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,269,275,280,281,282,283,284,286,288,290,291,292,293,294,295,296,297,298,299,300,301,302,303,304,305,306,307,308,309,310,311) then 0.05
                else 0
            end) as yoy_cap_premium
      -- Incumbent Renewal
      ,ren.brand as renewal_brand
      ,ren.scheme as renewal_scheme
      ,ren.netpremium + ren.sirncdamount as renewal_netpremium
      ,case when ren.netpremiumpncd = 0 then null else ren.netpremiumpncd + ren.sirpncdamount end as renewal_netpremiumpncd
      ,ren.commission as renewal_earnixcommission
      ,case when ren.netpremiumpncd = 0 then null else ren.commissionpncd end as renewal_earnixcommissionpncd
      ,case when ren.commission <= 0 then ren.commission - 20
            when ren.commission > 0 and ren.commission <= 20/1.12 then ren.commission * 1.12 - 20
            else ren.commission - 20/1.12 end as renewal_commission
      ,case when ren.netpremiumpncd = 0 then null else
        case when ren.commissionpncd <= 0 then ren.commissionpncd - 20
            when ren.commissionpncd > 0 and ren.commissionpncd <= 20/1.12 then ren.commissionpncd * 1.12 - 20
            else ren.commissionpncd - 20/1.12 end end as renewal_commissionpncd
      ,ren.netpremium + ren.sirncdamount + case when ren.commission <= 0 then ren.commission - 20
            when ren.commission > 0 and ren.commission <= 20/1.12 then ren.commission * 1.12 - 20
            else ren.commission - 20/1.12 end as renewal_grosspremium
      ,case when ren.netpremiumpncd = 0 then null else
        ren.netpremiumpncd + ren.sirpncdamount + case when ren.commissionpncd <= 0 then ren.commissionpncd - 20
            when ren.commissionpncd > 0 and ren.commissionpncd <= 20/1.12 then ren.commissionpncd * 1.12 - 20
            else ren.commissionpncd - 20/1.12 end end as renewal_grosspremiumpncd
      -- Incumbent Auto-Rebroke
      ,reb.brand as rebroke_brand
      ,reb.scheme as rebroke_scheme
      ,reb.referral as rebroke_referral
      ,reb.netpremium + reb.sirncdamount as rebroke_netpremium
      ,case when reb.netpremiumpncd = 0 then null else reb.netpremiumpncd + reb.sirpncdamount end as rebroke_netpremiumpncd
      ,reb.commission as rebroke_earnixcommission
      ,case when reb.netpremiumpncd = 0 then null else reb.commissionpncd end as rebroke_earnixcommissionpncd
      ,case when reb.commission <= 0 then reb.commission - 20
            when reb.commission > 0 and reb.commission <= 20/1.12 then reb.commission * 1.12 - 20
            else reb.commission - 20/1.12 end as rebroke_commission
      ,case when reb.netpremiumpncd = 0 then null else
        case when reb.commissionpncd <= 0 then reb.commissionpncd - 20
            when reb.commissionpncd > 0 and reb.commissionpncd <= 20/1.12 then reb.commissionpncd * 1.12 - 20
            else reb.commissionpncd - 20/1.12 end end as rebroke_commissionpncd
      ,reb.netpremium + reb.sirncdamount + case when reb.commission <= 0 then reb.commission - 20
            when reb.commission > 0 and reb.commission <= 20/1.12 then reb.commission * 1.12 - 20
            else reb.commission - 20/1.12 end as rebroke_grosspremium
      ,case when reb.netpremiumpncd = 0 then null else 
        reb.netpremiumpncd + reb.sirpncdamount + case when reb.commissionpncd <= 0 then reb.commissionpncd - 20
            when reb.commissionpncd > 0 and reb.commissionpncd <= 20/1.12 then reb.commissionpncd * 1.12 - 20
            else reb.commissionpncd - 20/1.12 end end as rebroke_grosspremiumpncd
      -- ENBP
      ,enb.brand as enbp_brand
      ,enb.scheme as enbp_scheme
      ,enb.netpremium + enb.sirncdamount as enbp_netpremium
      ,case when enb.netpremiumpncd = 0 then null else enb.netpremiumpncd + enb.sirpncdamount end as enbp_netpremiumpncd
      ,enb.commission as enbp_earnixcommission
      ,case when enb.netpremiumpncd = 0 then null else enb.commissionpncd end as enbp_earnixcommissionpncd
      ,case when enb.commission <= 0 then enb.commission - 20
            when enb.commission > 0 and enb.commission <= 20/1.12 then enb.commission * 1.12 - 20
            else enb.commission - 20/1.12 end as enbp_commission
      ,case when enb.netpremiumpncd = 0 then null else
        case when enb.commissionpncd <= 0 then enb.commissionpncd - 20
            when enb.commissionpncd > 0 and enb.commissionpncd <= 20/1.12 then enb.commissionpncd * 1.12 - 20
            else enb.commissionpncd - 20/1.12 end end as enbp_commissionpncd
      ,enb.netpremium + enb.sirncdamount + case when enb.commission <= 0 then enb.commission - 20
            when enb.commission > 0 and enb.commission <= 20/1.12 then enb.commission * 1.12 - 20
            else enb.commission - 20/1.12 end as enbp_grosspremium
      ,case when enb.netpremiumpncd = 0 then null else 
        enb.netpremiumpncd + enb.sirpncdamount + case when enb.commissionpncd <= 0 then enb.commissionpncd - 20
            when enb.commissionpncd > 0 and enb.commissionpncd <= 20/1.12 then enb.commissionpncd * 1.12 - 20
            else enb.commissionpncd - 20/1.12 end end as enbp_grosspremiumpncd
      -- Invited
      ,inv.brand as invited_brand
      ,inv.scheme as invited_scheme
      ,inv.netpremium + inv.sirncdamount as invited_netpremium
      ,case when inv.netpremiumpncd = 0 then null else inv.netpremiumpncd + inv.sirpncdamount end as invited_netpremiumpncd
      ,inv.commission as invited_earnixcommission
      ,case when inv.netpremiumpncd = 0 then null else inv.commissionpncd end as invited_earnixcommissionpncd
      ,case when inv.commission <= 0 then inv.commission - 20
            when inv.commission > 0 and inv.commission <= 20/1.12 then inv.commission * 1.12 - 20
            else inv.commission - 20/1.12 end as invited_commission
      ,case when inv.netpremiumpncd = 0 then null else
        case when inv.commissionpncd <= 0 then inv.commissionpncd - 20
            when inv.commissionpncd > 0 and inv.commissionpncd <= 20/1.12 then inv.commissionpncd * 1.12 - 20
            else inv.commissionpncd - 20/1.12 end end as invited_commissionpncd
      ,inv.netpremium + inv.sirncdamount + case when inv.commission <= 0 then inv.commission - 20
            when inv.commission > 0 and inv.commission <= 20/1.12 then inv.commission * 1.12 - 20
            else inv.commission - 20/1.12 end as invited_grosspremium
      ,case when inv.netpremiumpncd = 0 then null else
        inv.netpremiumpncd + inv.sirpncdamount + case when inv.commissionpncd <= 0 then inv.commissionpncd - 20
            when inv.commissionpncd > 0 and inv.commissionpncd <= 20/1.12 then inv.commissionpncd * 1.12 - 20
            else inv.commissionpncd - 20/1.12 end end as invited_grosspremiumpncd
      -- New Business
      ,new.brand as newbusiness_brand
      ,new.scheme as newbusiness_scheme
      ,new.netpremium + new.sirncdamount as newbusiness_netpremium
      ,case when new.netpremiumpncd = 0 then null else new.netpremiumpncd + new.sirpncdamount end as newbusiness_netpremiumpncd
      ,new.commission as newbusiness_earnixcommission
      ,case when new.netpremiumpncd = 0 then null else new.commissionpncd end as newbusiness_earnixcommissionpncd
      ,case when new.commission <= 0 then new.commission - 20
            when new.commission > 0 and new.commission <= 20/1.12 then new.commission * 1.12 - 20
            else new.commission - 20/1.12 end as newbusiness_commission
      ,case when new.netpremiumpncd = 0 then null else
        case when new.commissionpncd <= 0 then new.commissionpncd - 20
            when new.commissionpncd > 0 and new.commissionpncd <= 20/1.12 then new.commissionpncd * 1.12 - 20
            else new.commissionpncd - 20/1.12 end end as newbusiness_commissionpncd
      ,new.netpremium + new.sirncdamount + case when new.commission <= 0 then new.commission - 20
            when new.commission > 0 and new.commission <= 20/1.12 then new.commission * 1.12 - 20
            else new.commission - 20/1.12 end as newbusiness_grosspremium
      ,case when new.netpremiumpncd = 0 then null else
        new.netpremiumpncd + new.sirpncdamount + case when new.commissionpncd <= 0 then new.commissionpncd - 20
            when new.commissionpncd > 0 and new.commissionpncd <= 20/1.12 then new.commissionpncd * 1.12 - 20
            else new.commissionpncd - 20/1.12 end end as newbusiness_grosspremiumpncd
      ,new.pol_dti
      ,new.eci_dti
//      ,enb.covea_net + enb.sirncdamount as rn_covea_net
//      ,enb.covea_net_pncd + enb.sirpncdamount as rn_covea_net_pncd
from wrk_retailpricing.car.gipp_van_subs g
-- Incumbent renewal or IsCheapest where incumbent declined
left join earnix_rn ren
on g.rn_submission = ren.rn_submission
and (ren.isincumbent = 'true' or ren.ischeapest = 'true')
-- Auto-rebroke for incumbent insurer; null where incumbent decline
left join earnix_rn reb
on ren.rn_submission = reb.rn_submission
and ren.scheme = reb.scheme
and ren.brand = reb.brand
and ren.isincumbent = 'true'
and reb.isincumbent = 'false'
and reb.referral = 'N'
-- Cheapest auto-rebroke
left join enbp enb
on ren.rn_submission = enb.rn_submission
and enb.quote_no = 1
-- IsSelected invite
left join earnix_rn inv
on g.rn_submission = inv.rn_submission
and inv.isselected = 'true'
left join brand b
on g.nb_submission = b.nb_submission
-- Cheapest new business
left join nb new
on g.nb_submission = new.nb_submission
and new.quote_no = 1
where g.rn_submission is not null
;
    """
    auto_seven = """
    create or replace temporary table gipp_mon_cov_enbp as

select distinct g.*
      ,case when g.pncd = 'N' and (d.value = 'true' or (d.value is null and g.dd_duq = 'Y')) then cast(rc.netpremium as numeric(10,2)) * 1.025
            when g.pncd = 'N' and (d.value = 'false' or (d.value is null and g.dd_duq = 'N')) then cast(rc.netpremium as numeric(10,2)) * 0.95
            when g.pncd = 'Y' and (d.value = 'true' or (d.value is null and g.dd_duq = 'Y')) then cast(rc.netpremiumpncd as numeric(10,2)) * 1.025
            when g.pncd = 'Y' and (d.value = 'false' or (d.value is null and g.dd_duq = 'N')) then cast(rc.netpremiumpncd as numeric(10,2)) * 0.95
            else 0 end as covea_rn_net
      ,case when g.pncd = 'N' and (d.value = 'true' or (d.value is null and g.dd_duq = 'Y')) then cast(nc.netpremium as numeric(10,2)) * 1.025
            when g.pncd = 'N' and (d.value = 'false' or (d.value is null and g.dd_duq = 'N')) then cast(nc.netpremium as numeric(10,2)) * 0.95
            when g.pncd = 'Y' and (d.value = 'true' or (d.value is null and g.dd_duq = 'Y')) then cast(nc.netpremiumpncd as numeric(10,2)) * 1.025
            when g.pncd = 'Y' and (d.value = 'false' or (d.value is null and g.dd_duq = 'N')) then cast(nc.netpremiumpncd as numeric(10,2)) * 0.95
            else 0 end as covea_nb_net
      ,case when g.pncd = 'N' then cast(ra.netpremium as numeric(10,2))
            when g.pncd = 'Y' then cast(ra.netpremiumpncd as numeric(10,2)) 
            else 0 end as app_rn_net
      ,case when g.pncd = 'N' then cast(na.netpremium as numeric(10,2))
            when g.pncd = 'Y' then cast(na.netpremiumpncd as numeric(10,2)) 
            else 0 end as app_nb_net
      --,d.value
      from gipp_mon g
left join PRD_RAW_DB.QUOTES_PUBLIC.vw_earnix_req_scheme rc
on g.rn_submission = rc.quote_reference
and g.rn_date_created = rc.date_created
and g.enbp_brand = rc.brand
and g.rn_agghub_quote_version = rc.agghub_quote_version
and substring(rc.scheme,1,2) = 'CO'
left join PRD_RAW_DB.QUOTES_PUBLIC.vw_earnix_req_scheme nc
on g.nb_submission = nc.quote_reference
and g.nb_date_created = nc.date_created
and g.enbp_brand = nc.brand
and g.NB_agghub_quote_version = nc.agghub_quote_version
and substring(nc.scheme,1,2) = 'CO'
left join PRD_RAW_DB.QUOTES_PUBLIC.vw_earnix_req_scheme ra
on g.rn_submission = ra.quote_reference
and g.rn_date_created = ra.date_created
and g.enbp_brand = ra.brand
and g.rn_agghub_quote_version = ra.agghub_quote_version
and substring(ra.scheme,1,2) = 'AP'
left join PRD_RAW_DB.QUOTES_PUBLIC.vw_earnix_req_scheme na
on g.nb_submission = na.quote_reference
and g.nb_date_created = na.date_created
and g.enbp_brand = na.brand
and g.nb_agghub_quote_version = na.agghub_quote_version
and substring(na.scheme,1,2) = 'AP'
left join PRD_RAW_DB.QUOTES_PUBLIC.vw_earnix_req_dataenrichment d
on g.nb_submission = d.quote_reference
and g.nb_date_created = d.date_created
and d.type = 'ECI'
and d.key = 'InvitedPayment'
;
        """
    auto_eight = """
         -- Create final table with flags to help investigating
create or replace table gipp_mon_results as

with checks as (
select *
    -- Cheapest auto-rebroke net matches new business
      ,case when enbp_netpremium is null or newbusiness_netpremium is null then null
        when
        case when pncd = 'Y' then enbp_netpremiumpncd else enbp_netpremium end =
        case when pncd = 'Y' then newbusiness_netpremiumpncd else newbusiness_netpremium end
       then 'Y' else 'N' end as enbp_net_eq_nb_net
    -- Invited net less than or equal to new business
      ,case when invited_netpremium is null or newbusiness_netpremium is null then null
        when
        case when pncd = 'Y' then invited_netpremiumpncd else invited_netpremium end <=
        case when pncd = 'Y' then newbusiness_netpremiumpncd else newbusiness_netpremium end
       then 'Y' else 'N' end as inv_net_le_nb_net
    -- Renewal net less than or equal to same insurer auto-rebroke
      ,case when renewal_netpremium is null or rebroke_netpremium is null then null
        when
        case when pncd = 'Y' then renewal_netpremiumpncd else renewal_netpremium end <=
        case when pncd = 'Y' then rebroke_netpremiumpncd else rebroke_netpremium end
       then 'Y' else 'N' end as rn_net_le_rb_net
    -- Invited commission equals new business
      ,case when invited_commission is null or newbusiness_commission is null or invited_netpremium is null or newbusiness_netpremium is null then null
        when
        abs(case when pncd = 'Y' then invited_commissionpncd/invited_netpremiumpncd else invited_commission/invited_netpremium end -
        case when pncd = 'Y' then newbusiness_commissionpncd/newbusiness_netpremiumpncd else newbusiness_commission/newbusiness_netpremium end) < 0.0001
       then 'Y' else 'N' end as inv_comm_eq_nb_comm
    -- Invited commission less than new business
      ,case when invited_commission is null or newbusiness_commission is null or invited_netpremium is null or newbusiness_netpremium is null then null
        when
        case when pncd = 'Y' then invited_commissionpncd/invited_netpremiumpncd else invited_commission/invited_netpremium end <
        case when pncd = 'Y' then newbusiness_commissionpncd/newbusiness_netpremiumpncd else newbusiness_commission/newbusiness_netpremium end
       then 'Y' else 'N' end as inv_comm_lt_nb_comm
    -- Invited commission greater than new business
      ,case when invited_commission is null or newbusiness_commission is null or invited_netpremium is null or newbusiness_netpremium is null then null
        when
        case when pncd = 'Y' then invited_commissionpncd/invited_netpremiumpncd else invited_commission/invited_netpremium end >
        case when pncd = 'Y' then newbusiness_commissionpncd/newbusiness_netpremiumpncd else newbusiness_commission/newbusiness_netpremium end
       then 'Y' else 'N' end as inv_comm_gt_nb_comm
      -- Invited commission greater than auto-rebroke
      ,case when invited_commission is null or enbp_commission is null or invited_netpremium is null or enbp_netpremium is null then null
        when
        case when pncd = 'Y' then invited_commissionpncd/invited_netpremiumpncd else invited_commission/invited_netpremium end >
        case when pncd = 'Y' then enbp_commissionpncd/enbp_netpremiumpncd else enbp_commission/enbp_netpremium end
       then 'Y' else 'N' end as inv_comm_gt_enbp_comm
      -- YoY cap has applied
      ,case when invited_commission is null or enbp_commission is null or invited_netpremium is null or enbp_netpremium is null then null
        when
        case when pncd = 'Y' then abs(invited_commissionpncd + invited_netpremiumpncd - yoy_cap_premium) else abs(invited_commission + invited_netpremium - yoy_cap_premium) end < 
        0.01 * case when pncd = 'Y' then invited_netpremiumpncd else invited_netpremium end
       then 'Y' else 'N' end as inv_price_eq_yoy_cap
from gipp_mon_cov_enbp
),

checks_covea as (
select *
      ,case when invited_netpremium is null or invited_commission > 50000 then 'Renewal Declined'
            when newbusiness_netpremium is null or newbusiness_commission > 50000 then 'New Business Declined'
            when enbp_net_eq_nb_net = 'Y' and inv_net_le_nb_net = 'Y' and inv_comm_eq_nb_comm = 'Y' then 'No Errors'
            when enbp_net_eq_nb_net = 'Y' and inv_net_le_nb_net = 'Y' and inv_comm_lt_nb_comm = 'Y' then 'Commission Lower'
//            when enbp_net_eq_nb_net = 'N' and inv_net_le_nb_net = 'N' then 'Net Rate Issue'
            when enbp_net_eq_nb_net = 'Y' and inv_comm_gt_nb_comm = 'Y' then 'Commission Issue' else 'Net Rate Issue'

       end as result
from checks
),

checks_detail as (
select *
      ,case when result = 'Net Rate Issue' and substring(invited_scheme,1,2) = 'CO' and substring(newbusiness_scheme,1,2) = 'CO' and (pol_dti <> eci_dti) then 'Covea DTI Difference'
            when enbp_net_eq_nb_net = 'N' and inv_net_le_nb_net = 'N' then 'RN Net Higher'          
            when enbp_net_eq_nb_net = 'N' and inv_net_le_nb_net = 'Y' and substring(invited_scheme,1,2) = 'AD' and substring(newbusiness_scheme,1,2) = 'AD' then 'Adv ENBP Net Lower - Under Investigation'
            when enbp_net_eq_nb_net = 'N' and inv_net_le_nb_net = 'Y' and substring(invited_scheme,1,2) = 'AP' and substring(newbusiness_scheme,1,2) = 'AP' then 'APP ENBP Net Lower - Under Investigation'
            when result = 'Net Rate Issue' and substring(invited_scheme,1,2) <> substring(newbusiness_scheme,1,2) then 'Insurers different at RN and NB' else result
       end as result_more_detail_cte
from checks_covea
)

select *
      ,case when result_more_detail_cte = 'Net Rate Issue' and result_more_detail_cte = 'RN Net Higher' and substring(newbusiness_scheme,1,2) = 'CO' and covea_rn_net > (case when pncd = 'Y' then invited_netpremiumpncd else invited_netpremium end) then 'Covea DTI Difference' else result_more_detail_cte
       end as result_more_detail
       ,case when result = 'Commission Issue' then 'Y'
            when result_more_detail_cte = 'RN Net Higher' then 'Y' else 'N'
       end as compliance_issue
from checks_detail
;
        """
    auto_nine = """
    ALTER TABLE gipp_mon_results DROP COLUMN result_more_detail_cte;
        """
    auto_ten = """
    ALTER TABLE gipp_mon_results DROP COLUMN pol_dti;  
        """

    auto_eleven = """
    ALTER TABLE gipp_mon_results DROP COLUMN eci_dti;
            """
    cs.execute(auto_zero)
    cs.execute(auto_one)
    cs.execute(auto_two)
    cs.execute(auto_three)
    cs.execute(auto_four)
    cs.execute(auto_five)
    cs.execute(auto_six)
    cs.execute(auto_seven)
    # cs.execute("use database PRD_RAW_DB;")
    # cs.execute("use schema QUOTES_PUBLIC;")
    cs.execute(auto_eight)
    cs.execute(auto_nine)
    cs.execute(auto_ten)
    cs.execute(auto_eleven)
    results_fin = """
select distinct rn_submission
      ,nb_submission
      ,rn_date_created
      ,rn_date_created as nb_date_created
      ,pncd
      ,dd_duq
      ,rates
      ,nb_rates
      ,incumbent_present
      ,invited_incumbent
      ,yoy_cap_premium
      ,renewal_brand
      ,renewal_scheme
      ,renewal_netpremium
      ,renewal_netpremiumpncd
      ,renewal_earnixcommission
      ,renewal_earnixcommissionpncd
      ,renewal_commission
      ,renewal_commissionpncd
      ,renewal_grosspremium
      ,renewal_grosspremiumpncd
      ,rebroke_brand
      ,rebroke_scheme
      ,rebroke_referral
      ,rebroke_netpremium
      ,rebroke_netpremiumpncd
      ,rebroke_earnixcommission
      ,rebroke_earnixcommissionpncd
      ,rebroke_commission
      ,rebroke_commissionpncd
      ,rebroke_grosspremium
      ,rebroke_grosspremiumpncd
      ,enbp_brand
      ,enbp_scheme
      ,enbp_netpremium
      ,enbp_netpremiumpncd
      ,enbp_earnixcommission
      ,enbp_earnixcommissionpncd
      ,enbp_commission
      ,enbp_commissionpncd
      ,enbp_grosspremium
      ,enbp_grosspremiumpncd
      ,invited_brand
      ,invited_scheme
      ,invited_netpremium
      ,invited_netpremiumpncd
      ,invited_earnixcommission
      ,invited_earnixcommissionpncd
      ,invited_commission
      ,invited_commissionpncd
      ,invited_grosspremium
      ,invited_grosspremiumpncd
      ,newbusiness_brand
      ,newbusiness_scheme
      ,newbusiness_netpremium
      ,newbusiness_netpremiumpncd
      ,newbusiness_earnixcommission
      ,newbusiness_earnixcommissionpncd
      ,newbusiness_commission
      ,newbusiness_commissionpncd
      ,newbusiness_grosspremium
      ,newbusiness_grosspremiumpncd
      ,covea_rn_net
      ,covea_nb_net
      ,app_rn_net
      ,app_nb_net
      ,enbp_net_eq_nb_net
      ,inv_net_le_nb_net
      ,rn_net_le_rb_net
      ,inv_comm_eq_nb_comm
      ,inv_comm_lt_nb_comm
      ,inv_comm_gt_nb_comm
      ,inv_comm_gt_enbp_comm
      ,inv_price_eq_yoy_cap
      ,result
      ,result_more_detail
      ,compliance_issue
from gipp_mon_results
order by rn_submission;
        """
    cs.execute(results_fin)
    try:
        df = cs.fetch_pandas_all()
        df.to_csv("../results/res11.csv")
    finally:
        print("done")


def get_dataissue_file(cs):
    auto_one = """
create or replace temporary table earnix_id_hus_nb as
WITH NB AS(
SELECT V.NB_SUBMISSION
      ,V.NB_DATE_CREATED AS DATE_CREATED
      ,B.nk_agg_id_quote_ref
      ,MIN(B.AGGHUB_ID) AS AGGHUB_ID_REQ_E
     ,MIN(P.AGGHUB_ID) AS AGGHUB_ID_REQ_P
     ,MIN(R.AGGHUB_ID) AS AGGHUB_ID_RES_R
FROM WRK_RETAILPRICING.CAR.GIPP_VAN_SUBS V
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
    """
    auto_two = """
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
    """
    auto_three = """
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
    """
    auto_four = """
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
    """
    auto_five = """
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
    """
    auto_six = """
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
    """
    auto_seven = """
create or replace temporary table earnix_id_hus_rn as
WITH RN AS(
SELECT V.RN_SUBMISSION
      ,V.RN_DATE_CREATED AS DATE_CREATED
      ,B.nk_agg_id_quote_ref
      ,MIN(B.AGGHUB_ID) AS AGGHUB_ID_REQ_E
      ,MIN(P.AGGHUB_ID) AS AGGHUB_ID_REQ_P
      ,MIN(R.AGGHUB_ID) AS AGGHUB_ID_RES_R
FROM WRK_RETAILPRICING.CAR.GIPP_VAN_SUBS V
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
       """
    auto_eight = """
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

        """
    auto_nine = """
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
        """
    auto_ten = """
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
        """
    auto_eleven = """
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
            """
    auto_twelve = """
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
            """
    auto_thirteen = """
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
      --,case when P.INSTALMENTS = v.INSTALMENTS  then 0 else 1 end as INSTALMENTS
      --,case when P.DEPOSIT = v.DEPOSIT  then 0 else 1 end as DEPOSIT
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
from WRK_RETAILPRICING.CAR.GIPP_VAN_SUBS g
inner join earnix_nb_base_huss_fin p
on g.nb_submission = p.nb_submission
inner join earnix_rn_base_huss_fin v
on g.rn_submission = v.rn_submission
;
            """

    auto_fourteen = """
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
from WRK_RETAILPRICING.CAR.GIPP_VAN_SUBS g
inner join earnix_nb_driver_huss p
on g.nb_submission = p.nb_submission
inner join earnix_rn_driver_huss v
on g.rn_submission = v.rn_submission
and p.driverprn = v.driverprn
;

                """
    auto_fifteen = """
create or replace temporary table check_conviction_van as
select g.*
,r.driverprn
,case when r.CONVICTIONCODE = n.CONVICTIONCODE then 0 else 1 end as code
,case when r.CONVICTIONDATE = n.CONVICTIONDATE then 0 else 1 end as date
from WRK_RETAILPRICING.CAR.GIPP_VAN_SUBS g
inner join earnix_rn_convic_huss r
on g.rn_submission = r.rn_submission
left join earnix_nb_convic_huss n
on g.nb_submission = n.nb_submission
and r.driverprn = n.driverprn
;
                    """

    auto_sixteen = """
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
                    """

    auto_seventeen = """
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
                       """

    auto_eighteen = """
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
                       """
#     results_fin = """
# -- Fuller version to aid debugging
# create or replace temporary table check_summary_full as
# with driver_rn_cnt as (
# select rn_submission
#       ,count(driverprn) as driver_count
# from earnix_rn_driver
# group by rn_submission
# ),
#
# driver_nb_cnt as (
# select nb_submission
#       ,count(driverprn) as driver_count
# from earnix_nb_driver
# group by nb_submission
# ),
#
# driver_cte as (
# select rn_submission
#       ,nb_submission
#       ,sum(dateofbirth) as driver_dateofbirth
#       ,sum(age)as driver_age
#       ,sum(maritalstatus) as driver_maritalstatus
#       ,sum(ukresidencydate) as ukresidencydate
#       ,sum(ukresidentyears) as ukresidentyears
#       ,sum(ukresidentfrombirth) as ukresidentfrombirth
#       ,sum(nonmotorconvictions) as nonmotorconvictions
#       ,sum(accessothervehicles) as accessothervehicles
#       ,sum(medicalcondition) as medicalcondition
#       ,sum(relationshiptoproposer) as relationshiptoproposer
#       ,sum(drivesvehicle) as drivesvehicle
#       ,sum(licenceyears) as licenceyears
#       ,sum(licencemonths) as licencemonths
#       ,sum(licencetype) as licencetype
#       ,sum(mylicenceind) as mylicenceind
#       ,sum(mylicenceresult) as mylicenceresult
#       ,sum(passplusind) as passplusind
#       ,sum(employersbusiness_full) as employersbusiness_full
#       ,sum(occupationcode_full) as occupationcode_full
#       ,sum(employmenttype_full) as employmenttype_full
#       ,sum(employersbusiness_part) as employersbusiness_part
#       ,sum(occupationcode_part) as occupationcode_part
#       ,sum(employmenttype_part) as employmenttype_part
# from check_driver
# group by rn_submission, nb_submission
# ),
#
# claim_rn_cnt as (
# select rn_submission
#       ,count(*) as claim_count
# from earnix_rn_claim
# group by rn_submission
# ),
#
# claim_nb_cnt as (
# select nb_submission
#       ,count(*) as claim_count
# from earnix_nb_claim
# group by nb_submission
# ),
#
# conv_rn_cnt as (
# select rn_submission
#       ,count(*) as conv_count
# from earnix_rn_conviction
# group by rn_submission
# ),
#
# conv_nb_cnt as (
# select nb_submission
#       ,count(*) as conv_count
# from earnix_nb_conviction
# group by nb_submission
# ),
#
# claim_cte as (
# select rn_submission
#       ,nb_submission
#       ,sum(claim_match) as nb_claim_missing
#       ,sum(cost_match) as nb_cost_wrong
# from check_claim
# group by rn_submission, nb_submission
# ),
#
# conv_cte as (
# select rn_submission
#       ,nb_submission
#       ,sum(code) as code_wrong
#       ,sum(date) as date_wrong
# from check_conviction
# group by rn_submission, nb_submission
# ),
#
# eci_cte as (
# select rn_submission
#       ,nb_submission
#       ,sum(daystoinception) as dti_issue
#       ,sum(cuepiscore+cuescore) as cue_issue
#       ,sum(paymenttype+instalmentsrequestedind) as payment_issue
# from check_eci
# group by rn_submission, nb_submission
# )
#
# select b.*
#       ,e.dti_issue
#       ,e.cue_issue
#       ,e.payment_issue
#       ,case when dr.driver_count != dn.driver_count then 1 else 0 end as driver_num_issue
#       ,d.driver_dateofbirth
#       ,d.driver_age
#       ,d.driver_maritalstatus
#       ,d.ukresidencydate
#       ,d.ukresidentyears
#       ,d.ukresidentfrombirth
#       ,d.nonmotorconvictions
#       ,d.accessothervehicles
#       ,d.medicalcondition
#       ,d.relationshiptoproposer
#       ,d.drivesvehicle
#       ,d.licenceyears
#       ,d.licencemonths
#       ,d.licencetype
#       ,d.mylicenceind
#       ,d.mylicenceresult
#       ,d.passplusind
#       ,d.employersbusiness_full
#       ,d.occupationcode_full
#       ,d.employmenttype_full
#       ,d.employersbusiness_part
#       ,d.occupationcode_part
#       ,d.employmenttype_part
#       ,case when nvl(clr.claim_count,0) != nvl(cln.claim_count,0) then 1 else 0 end as claim_num_issue
#       ,cl.nb_claim_missing
#       ,cl.nb_cost_wrong as nb_claim_wrong
#       ,case when nvl(cnr.conv_count,0) != nvl(cnn.conv_count,0) then 1 else 0 end as conv_num_issue
#       ,cn.code_wrong as conv_code_wrong
#       ,cn.date_wrong as conv_date_wrong
# from check_base b
# left join driver_rn_cnt dr
# on b.rn_submission = dr.rn_submission
# left join driver_nb_cnt dn
# on b.nb_submission = dn.nb_submission
# left join driver_cte d
# on b.rn_submission = d.rn_submission
# left join claim_rn_cnt clr
# on b.rn_submission = clr.rn_submission
# left join claim_nb_cnt cln
# on b.nb_submission = cln.nb_submission
# left join claim_cte cl
# on b.rn_submission = cl.rn_submission
# left join conv_rn_cnt cnr
# on b.rn_submission = cnr.rn_submission
# left join conv_nb_cnt cnn
# on b.nb_submission = cnn.nb_submission
# left join conv_cte cn
# on b.rn_submission = cn.rn_submission
# left join eci_cte e
# on b.nb_submission = e.nb_submission
# ;
#         """
    cs.execute(auto_one)
    cs.execute(auto_two)
    cs.execute(auto_three)
    cs.execute(auto_four)
    cs.execute(auto_five)
    cs.execute(auto_six)
    cs.execute(auto_seven)
    cs.execute(auto_eight)
    cs.execute(auto_nine)
    cs.execute(auto_ten)
    cs.execute(auto_eleven)
    cs.execute(auto_twelve)
    cs.execute(auto_thirteen)
    cs.execute(auto_fourteen)
    cs.execute(auto_fifteen)
    cs.execute(auto_sixteen)
    cs.execute(auto_seventeen)
    cs.execute(auto_eighteen)
    # cs.execute(results_fin)
    results_fin = """
select distinct * from check_summary_full
order by rn_submission asc;
        """
    cs.execute(results_fin)
    try:
        df = cs.fetch_pandas_all()
        df.to_csv("../results/data_issues.csv")
    finally:
        print("done")
