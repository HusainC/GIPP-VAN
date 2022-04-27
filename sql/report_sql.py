import pandas as pd
from snowflake.connector.pandas_tools import write_pandas


def get_results_file(cs):
    sql = "USE ROLE DRS_QUOTEPAYLOAD"
    cs.execute(sql)
    sql = "use warehouse prd_quotes_medium;"
    cs.execute(sql)
    sql = "use database PRD_QUOTES;"
    cs.execute(sql)
    sql = "use schema QUOTE_PAYLOAD;"
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
from demo_db.public.gipp_mon_subs g
left join prd_quotes.quote_payload.vw_earnix_req_base b
on g.rn_submission = b.quote_reference and b.date_created >= '2021-12-01'
),
nb as (
select g.nb_submission
      ,b.date_created as nb_date_created
      ,row_number() over(partition by g.nb_submission
                        order by b.date_created asc) as date_no
from demo_db.public.gipp_mon_subs g
left join prd_quotes.quote_payload.vw_earnix_req_base b
on g.nb_submission = b.quote_reference and b.date_created >= '2021-12-01'
)
select g.rn_submission
      ,g.nb_submission
      ,rnl.rn_date_created
      ,nb.nb_date_created
from demo_db.public.gipp_mon_subs g
left join rnl on g.rn_submission = rnl.rn_submission and rnl.date_no = 1
left join nb on g.nb_submission = nb.nb_submission and nb.date_no = 1
;
    """
    auto_one = """
create or replace table demo_db.public.gipp_mon_subs as
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
from demo_db.public.gipp_mon_subs g
left join prd_quotes.quote_payload.vw_earnix_req_scheme q
on g.rn_submission = q.quote_reference
and g.rn_date_created = q.date_created
left join prd_quotes.quote_payload.vw_earnix_res_scheme r
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
left join prd_quotes.quote_payload.vw_earnix_req_scheme q
on e.request_id = q.agghub_id
and e.rn_date_created = q.date_created
left join prd_quotes.quote_payload.vw_earnix_req_policyproposer p
on q.agghub_id = p.agghub_id
and q.date_created = p.date_created
left join prd_quotes.quote_payload.vw_earnix_req_vehiclecover v
on q.agghub_id = v.agghub_id
and q.date_created = v.date_created
left join prd_quotes.quote_payload.vw_earnix_res_scheme r
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
from demo_db.public.gipp_mon_subs g
left join prd_quotes.quote_payload.vw_earnix_req_scheme q
on g.nb_submission = q.quote_reference
and g.nb_date_created = q.date_created
left join prd_quotes.quote_payload.vw_earnix_res_scheme r
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
left join prd_quotes.quote_payload.vw_earnix_req_dataenrichment d
on e.request_id = d.agghub_id
and e.nb_date_created = d.date_created
and d.type = 'ECI'
and d.key = 'InvitedPayment'
--where e.earnix_no = 1
left join prd_quotes.quote_payload.vw_earnix_req_dataenrichment dti
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
left join prd_quotes.quote_payload.vw_earnix_req_scheme q
on e.request_id = q.agghub_id
and e.nb_date_created = q.date_created
left join prd_quotes.quote_payload.vw_earnix_req_policyproposer p
on e.request_id = p.agghub_id
and e.nb_date_created = p.date_created
left join prd_quotes.quote_payload.vw_earnix_req_vehiclecover v
on e.request_id = v.agghub_id
and e.nb_date_created = v.date_created
left join prd_quotes.quote_payload.vw_earnix_res_scheme r
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
from demo_db.public.gipp_mon_subs g
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
from demo_db.public.gipp_mon_subs g
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
left join prd_quotes.quote_payload.vw_earnix_req_scheme rc
on g.rn_submission = rc.quote_reference
and g.rn_date_created = rc.date_created
and g.enbp_brand = rc.brand
and g.rn_agghub_quote_version = rc.agghub_quote_version
and substring(rc.scheme,1,2) = 'CO'
left join prd_quotes.quote_payload.vw_earnix_req_scheme nc
on g.nb_submission = nc.quote_reference
and g.nb_date_created = nc.date_created
and g.enbp_brand = nc.brand
and g.NB_agghub_quote_version = nc.agghub_quote_version
and substring(nc.scheme,1,2) = 'CO'
left join prd_quotes.quote_payload.vw_earnix_req_scheme ra
on g.rn_submission = ra.quote_reference
and g.rn_date_created = ra.date_created
and g.enbp_brand = ra.brand
and g.rn_agghub_quote_version = ra.agghub_quote_version
and substring(ra.scheme,1,2) = 'AP'
left join prd_quotes.quote_payload.vw_earnix_req_scheme na
on g.nb_submission = na.quote_reference
and g.nb_date_created = na.date_created
and g.enbp_brand = na.brand
and g.nb_agghub_quote_version = na.agghub_quote_version
and substring(na.scheme,1,2) = 'AP'
left join prd_quotes.quote_payload.vw_earnix_req_dataenrichment d
on g.nb_submission = d.quote_reference
and g.nb_date_created = d.date_created
and d.type = 'ECI'
and d.key = 'InvitedPayment'
;
        """
    auto_eight = """
         -- Create final table with flags to help investigating
create or replace table demo_db.public.gipp_mon_results as

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
    ALTER TABLE demo_db.public.gipp_mon_results DROP COLUMN result_more_detail_cte;
        """
    auto_ten = """
    ALTER TABLE demo_db.public.gipp_mon_results DROP COLUMN pol_dti;  
        """

    auto_eleven = """
    ALTER TABLE demo_db.public.gipp_mon_results DROP COLUMN eci_dti;
            """
    cs.execute(auto_zero)
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
from demo_db.public.gipp_mon_results
order by rn_submission;
        """
    cs.execute(results_fin)
    try:
        df = cs.fetch_pandas_all()
        df.to_csv("../results/res11.csv")
    finally:
        print("done")


def get_insurer_file(cs):
    auto_one = """
select g.*
      ,en.pncd
      ,en.brand
      ,en.scheme
      ,en.netpremium as enbp_netpremium
      ,en.netpremiumpncd as enbp_netpremiumpncd
      ,en.referral as enbp_referral
      ,nb.netpremium as newbusiness_netpremium
      ,nb.netpremiumpncd as newbusiness_netpremiumpncd
      ,nb.referral as newbusiness_referral
      ,case when g.rn_date_created = g.nb_date_created then 'Y' else 'N' end as match_date
      ,case when en.netpremium != nb.netpremium then 'N' else 'Y' end as match
      ,case when en.netpremiumpncd != nb.netpremiumpncd then 'N' else 'Y' end as match_pncd
      ,case when (en.referral = 'Y' and nb.referral is null) or (en.referral = 'N' and nb.referral = 'N') then 'Y' else 'N' end as match_referral
from demo_db.public.gipp_mon_subs g
-- Auto-rebroke
left outer join earnix_rn en
on g.rn_submission = en.rn_submission
and en.isincumbent != 'true'
-- New business
left outer join earnix_nb nb
on g.nb_submission = nb.nb_submission
and en.scheme = nb.scheme
and en.brand = nb.brand
where g.rn_date_created is not null
order by g.rn_submission, en.scheme
;
        """
    cs.execute(auto_one)
    try:
        df = cs.fetch_pandas_all()
        df.to_csv("../results/insurer11.csv")
    finally:
        print("done")


def get_dataissue_file(cs):
    auto_one = """
create or replace temporary table earnix_id as
select g.rn_submission
      ,g.rn_date_created
      ,b.agghub_id
      ,row_number() over(partition by g.rn_submission
                        order by cast(b.inserttimestamp as timestamp) asc) as earnix_no
from demo_db.public.gipp_mon_subs g
left join prd_quotes.quote_payload.vw_earnix_req_base b
on g.rn_submission = b.quote_reference
and g.rn_date_created = b.date_created
where g.rn_submission is not null
and b.payload_source != 'SOR-Renewal'
;
    """
    auto_two = """
    -- Get Earnix renewal request details
-- PolicyProposer and VehicleCover
create or replace temporary table earnix_rn_base as
select e.rn_submission
      ,e.rn_date_created
-- PolicyProposer
,p.pricingrandomid
,p.currentdatetime
,p.inceptiondate
,p.effectivedate
,p.enddate
,p.daystoinception
,p.brokertenure
,p.replacementpolicyind
,p.existingcustomer
,p.accounttype
,p.originalchanneltype
,p.originalchannelcode
--,p.campaigncode
,p.multicarind
,p.emaildomain
,p.dateofbirth
,p.age
,p.maritalstatus
,p.postcodefull
,p.postcodesector
,p.homeownerind
,p.timeataddress
,p.noofchildren
,p.noofdriversinfamily
,p.noofvehiclesavailabletofamily
,p.insurancerefused
,p.instalmentsrequestedind
,p.cuescore
,p.cuepiscore
-- TopX excluded
-- Old ECI excluded
,p.apr
,p.instalments
,p.deposit
,p.userrole
,p.propertymatchpolicy
,p.propertystringpolicy
,p.competitorname
,p.competitorpaymenttype
,p.competitorprice
,p.loyaltyreason
,p.onlineoriginal
,p.onlinecurrent
-- VehicleCover
,v.covercode
,v.coverperiod
,v.volxsamt
--,v.polarisgrantedvoluntaryexcess
,v.classofuse
,v.driversallowed
,v.annualmileage
,v.ownership
,v.keeper
,v.postcodefull as postcodefull_risk
,v.postcodesector as postcodesector_risk
,v.locationkeptovernight
,v.purchasedate
,v.firstregdyear
,v.vehicleage
,v.ownedyears
,v.ownedmonths
,v.value
,v.manufacturer
,v.model
,v.bodytype
,v.noofseats
,v.cubiccapacity
,v.fueltype
,v.transmissiontype
,v.leftorrighthanddrive
,v.fittedaeb
,v.modifiedind
,v.personalimportind
,v.alarmimmobiliser
,v.trackerdevicefittedind
,v.claimedyears
,v.claimedentitlementreason
,v.claimedyearsearned
,v.claimedprotectionreqdind
,v.hpivfs
,v.hpihri
,v.hpikeepers
,v.hpiowned
,v.creditmatch
,v.creditqcb
,v.creditscore
,v.idscore
,v.idscorepanel
,v.firstseendate
,v.seentoinception
,v.seentocurrent
,v.propertymatchrisk
,v.propertystringrisk
--,v.valuationdate
--,v.valuationstring
from earnix_id e
left join prd_quotes.quote_payload.vw_earnix_req_policyproposer p
on e.agghub_id = p.agghub_id
and e.rn_date_created = p.date_created
left join prd_quotes.quote_payload.vw_earnix_req_vehiclecover v
on e.agghub_id = v.agghub_id
and e.rn_date_created = v.date_created
and v.vehicleprn = '1'
where e.earnix_no = 1
;
    """
    auto_three = """
    -- Driver, Occupation and Drives
-- Need to handle mylicence!
create or replace temporary table earnix_rn_driver as
select e.rn_submission
      ,e.rn_date_created
-- Driver
,d.driverprn
,d.dateofbirth
,d.age
,d.maritalstatus
,d.ukresidencydate
,d.ukresidentyears
,d.ukresidentfrombirth
,d.nonmotorconvictions
,d.accessothervehicles
,nvl(d.licenceyears,0) as licenceyears
,nvl(d.licencemonths,0) as licencemonths
,nvl(d.licencetype,'MYLIC') as licencetype
,d.mylicenceind
,nvl(d.mylicenceresult,'false') as mylicenceresult
,d.passplusind
,d.medicalcondition
,d.ratedoccupation
,d.demeritpoints
-- Occupation
,o1.employersbusiness as employersbusiness_full
,o1.occupationcode as occupationcode_full
,o1.employmenttype as employmenttype_full
,nvl(o2.employersbusiness,'none') as employersbusiness_part
,nvl(o2.occupationcode,'none') as occupationcode_part
,nvl(o2.employmenttype,'none') as employmenttype_part
-- Drives
,dr.relationshiptoproposer
,dr.drivesvehicle
from earnix_id e
left join prd_quotes.quote_payload.vw_earnix_req_driver d
on e.agghub_id = d.agghub_id
and e.rn_date_created = d.date_created
left join prd_quotes.quote_payload.vw_earnix_req_occupation o1
on d.agghub_id = o1.agghub_id
and d.driverprn = o1.driverprn
and o1.fulltimeemploymentind = 'Y'
and d.date_created = o1.date_created
left join prd_quotes.quote_payload.vw_earnix_req_occupation o2
on d.agghub_id = o2.agghub_id
and d.driverprn = o2.driverprn
and o2.fulltimeemploymentind = 'N'
and d.date_created = o2.date_created
left join prd_quotes.quote_payload.vw_earnix_req_drives dr
on d.agghub_id = dr.agghub_id
and d.driverprn = dr.driverprn
and d.date_created = dr.date_created
and dr.vehicleprn = '1'
where e.earnix_no = 1
;
    """
    auto_four = """
-- Claim
create or replace temporary table earnix_rn_claim as
select e.rn_submission
      ,e.rn_date_created
,c.driverprn
,c.type
,c.date
,c.cost
,c.fault
from earnix_id e
inner join prd_quotes.quote_payload.vw_earnix_req_claim c
on e.agghub_id = c.agghub_id
and e.rn_date_created = c.date_created
where e.earnix_no = 1
and c.date_created <= dateadd(year, 5, c.date)
;
    """
    auto_five = """
    -- Conviction
-- Need to handle mylicence!
create or replace temporary table earnix_rn_conviction as
select e.rn_submission
      ,e.rn_date_created
,c.driverprn
,nvl(c.code,'MYLI') as code
,nvl(c.date,'1970-01-01') as date
,row_number() over(partition by e.rn_submission
                   order by cast(c.driverprn as smallint) asc
                  ,nvl(c.code,'MYLI') asc
                  ,cast(nvl(c.date,'1970-01-01') as date) asc) as conv_no
from earnix_id e
inner join prd_quotes.quote_payload.vw_earnix_req_conviction c
on e.agghub_id = c.agghub_id
and e.rn_date_created = c.date_created
where e.earnix_no = 1
and c.date_created <= dateadd(year, 5, c.date)
;
    """
    auto_six = """
    -- Get the Earnix new business request IDs
create or replace temporary table earnix_id as
select g.nb_submission
      ,g.nb_date_created
      ,b.agghub_id
      ,row_number() over(partition by g.nb_submission
                        order by cast(b.inserttimestamp as timestamp) asc) as earnix_no
from demo_db.public.gipp_mon_subs g
left join prd_quotes.quote_payload.vw_earnix_req_base b
on g.nb_submission = b.quote_reference
and g.nb_date_created = b.date_created
where g.nb_submission is not null
;
    """
    auto_seven = """
    -- Get Earnix new business details
-- PolicyProposer and VehicleCover
create or replace temporary table earnix_nb_base as
select e.nb_submission
      ,e.nb_date_created
-- PolicyProposer
,p.pricingrandomid
,p.currentdatetime
,p.inceptiondate
,p.effectivedate
,p.enddate
,p.daystoinception
,p.brokertenure
,p.replacementpolicyind
,p.existingcustomer
,p.accounttype
,p.originalchanneltype
,p.originalchannelcode
--,p.campaigncode
,p.multicarind
,p.emaildomain
,p.dateofbirth
,p.age
,p.maritalstatus
,p.postcodefull
,p.postcodesector
,p.homeownerind
,p.timeataddress
,p.noofchildren
,p.noofdriversinfamily
,p.noofvehiclesavailabletofamily
,p.insurancerefused
,p.instalmentsrequestedind
,p.cuescore
,p.cuepiscore
-- TopX excluded
-- Old ECI excluded
,p.apr
,p.instalments
,p.deposit
,p.userrole
,p.propertymatchpolicy
,p.propertystringpolicy
,p.competitorname
,p.competitorpaymenttype
,p.competitorprice
,p.loyaltyreason
,p.onlineoriginal
,p.onlinecurrent
-- VehicleCover
,v.covercode
,v.coverperiod
,v.volxsamt
--,v.polarisgrantedvoluntaryexcess
,v.classofuse
,v.driversallowed
,v.annualmileage
,v.ownership
,v.keeper
,v.postcodefull as postcodefull_risk
,v.postcodesector as postcodesector_risk
,v.locationkeptovernight
,v.purchasedate
,v.firstregdyear
,v.vehicleage
,v.ownedyears
,v.ownedmonths
,v.value
,v.manufacturer
,v.model
,v.bodytype
,v.noofseats
,v.cubiccapacity
,v.fueltype
,v.transmissiontype
,v.leftorrighthanddrive
,v.fittedaeb
,v.modifiedind
,v.personalimportind
,v.alarmimmobiliser
,v.trackerdevicefittedind
,v.claimedyears
,v.claimedentitlementreason
,v.claimedyearsearned
,v.claimedprotectionreqdind
,v.hpivfs
,v.hpihri
,v.hpikeepers
,v.hpiowned
,v.creditmatch
,v.creditqcb
,v.creditscore
,v.idscore
,v.idscorepanel
,v.firstseendate
,v.seentoinception
,v.seentocurrent
,v.propertymatchrisk
,v.propertystringrisk
--,v.valuationdate
--,v.valuationstring
from earnix_id e
left join prd_quotes.quote_payload.vw_earnix_req_policyproposer p
on e.agghub_id = p.agghub_id
and e.nb_date_created = p.date_created
left join prd_quotes.quote_payload.vw_earnix_req_vehiclecover v
on e.agghub_id = v.agghub_id
and e.nb_date_created = v.date_created
and v.vehicleprn = '1'
where e.earnix_no = 1
;       """
    auto_eight = """
    -- Driver, Occupation and Drives
-- Need to handle mylicence!
create or replace temporary table earnix_nb_driver as
select e.nb_submission
      ,e.nb_date_created
-- Driver
,d.driverprn
,d.dateofbirth
,d.age
,d.maritalstatus
,d.ukresidencydate
,d.ukresidentyears
,d.ukresidentfrombirth
,d.nonmotorconvictions
,d.accessothervehicles
,nvl(d.licenceyears,0) as licenceyears
,nvl(d.licencemonths,0) as licencemonths
,nvl(d.licencetype,'MYLIC') as licencetype
,d.mylicenceind
,nvl(d.mylicenceresult,'false') as mylicenceresult
,d.passplusind
,d.medicalcondition
,d.ratedoccupation
,d.demeritpoints
-- Occupation
,o1.employersbusiness as employersbusiness_full
,o1.occupationcode as occupationcode_full
,o1.employmenttype as employmenttype_full
,nvl(o2.employersbusiness,'none') as employersbusiness_part
,nvl(o2.occupationcode,'none') as occupationcode_part
,nvl(o2.employmenttype,'none') as employmenttype_part
-- Drives
,dr.relationshiptoproposer
,dr.drivesvehicle
from earnix_id e
left join prd_quotes.quote_payload.vw_earnix_req_driver d
on e.agghub_id = d.agghub_id
and e.nb_date_created = d.date_created
left join prd_quotes.quote_payload.vw_earnix_req_occupation o1
on d.agghub_id = o1.agghub_id
and d.driverprn = o1.driverprn
and o1.fulltimeemploymentind = 'Y'
and d.date_created = o1.date_created
left join prd_quotes.quote_payload.vw_earnix_req_occupation o2
on d.agghub_id = o2.agghub_id
and d.driverprn = o2.driverprn
and o2.fulltimeemploymentind = 'N'
and d.date_created = o2.date_created
left join prd_quotes.quote_payload.vw_earnix_req_drives dr
on d.agghub_id = dr.agghub_id
and d.driverprn = dr.driverprn
and d.date_created = dr.date_created
and dr.vehicleprn = '1'
where e.earnix_no = 1
;

        """
    auto_nine = """
        -- Claim
create or replace temporary table earnix_nb_claim as
select e.nb_submission
      ,e.nb_date_created
,c.driverprn
,c.type
,c.date
,c.cost
,c.fault
from earnix_id e
inner join prd_quotes.quote_payload.vw_earnix_req_claim c
on e.agghub_id = c.agghub_id
and e.nb_date_created = c.date_created
where e.earnix_no = 1
;
        """
    auto_ten = """
    -- Conviction
-- Need to handle mylicence!
create or replace temporary table earnix_nb_conviction as
select e.nb_submission
,e.nb_date_created
,c.driverprn
,nvl(c.code,'MYLI') as code
,nvl(c.date,'1970-01-01') as date
,row_number() over(partition by e.nb_submission
                   order by cast(c.driverprn as smallint) asc
                  ,nvl(c.code,'MYLI') asc
                  ,cast(nvl(c.date,'1970-01-01') as date) asc) as conv_no
from earnix_id e
inner join prd_quotes.quote_payload.vw_earnix_req_conviction c
on e.agghub_id = c.agghub_id
and e.nb_date_created = c.date_created
where e.earnix_no = 1
;
        """
    auto_eleven = """
    -- ECI
create or replace temporary table earnix_nb_eci as
select e.nb_submission
,e.nb_date_created
,d.key
,d.value
from earnix_id e
inner join prd_quotes.quote_payload.vw_earnix_req_dataenrichment d
on e.agghub_id = d.agghub_id
and e.nb_date_created = d.date_created
where e.earnix_no = 1
and d.type = 'ECI'
;
            """
    auto_twelve = """
    -- Get checking
create or replace temporary table check_base as
select g.rn_submission
,g.nb_submission
,g.rn_date_created
,g.nb_date_created as nb_date_created
,case when r.pricingrandomid = n.pricingrandomid then 0 else 1 end as pricingrandomid
,0 as currentdatetime
--,case when r.inceptiondate = n.inceptiondate then 0 else 1 end as inceptiondate
,case when r.effectivedate = n.effectivedate then 0 else 1 end as effectivedate
--,case when r.enddate = n.enddate then 0 else 1 end as enddate
--,case when r.daystoinception = n.daystoinception then 0 else 1 end as daystoinception
--,case when r.brokertenure = n.brokertenure then 0 else 1 end as brokertenure
--,case when r.replacementpolicyind = n.replacementpolicyind then 0 else 1 end as replacementpolicyind
--,case when r.existingcustomer = n.existingcustomer then 0 else 1 end as existingcustomer
--,case when r.accounttype = n.accounttype then 0 else 1 end as accounttype
,case when r.originalchanneltype = n.originalchanneltype then 0 else 1 end as originalchanneltype
,case when r.originalchannelcode = n.originalchannelcode then 0 else 1 end as originalchannelcode
--,case when r.campaigncode = n.campaigncode then 0 else 1 end as campaigncode
--,case when r.multicarind = n.multicarind then 0 else 1 end as multicarind
--,case when r.emaildomain = n.emaildomain then 0 else 1 end as emaildomain
,case when r.dateofbirth = n.dateofbirth then 0 else 1 end as dateofbirth
,case when r.age = n.age then 0 else 1 end as age
,case when r.maritalstatus = n.maritalstatus then 0 else 1 end as maritalstatus
,case when replace(r.postcodefull,' ','') = replace(n.postcodefull,' ','') then 0 else 1 end as postcodefull
,case when replace(r.postcodesector,' ','') = replace(n.postcodesector,' ','') then 0 else 1 end as postcodesector
,case when r.homeownerind = n.homeownerind then 0 else 1 end as homeownerind
--,case when r.timeataddress = n.timeataddress then 0 else 1 end as timeataddress
,case when r.noofchildren = n.noofchildren then 0 else 1 end as noofchildren
--,case when nvl(r.noofdriversinfamily,'X') = nvl(n.noofdriversinfamily,'X') then 0 else 1 end as noofdriversinfamily
,case when r.noofvehiclesavailabletofamily = n.noofvehiclesavailabletofamily then 0 else 1 end as noofvehiclesavailabletofamily
,case when r.insurancerefused = n.insurancerefused then 0 else 1 end as insurancerefused
--,case when r.instalmentsrequestedind = n.instalmentsrequestedind then 0 else 1 end as instalmentsrequestedind
--,case when r.cuescore = n.cuescore then 0 else 1 end as cuescore
--,case when r.cuepiscore = n.cuepiscore then 0 else 1 end as cuepiscore
--,case when r.apr = n.apr then 0 else 1 end as apr
--,case when r.instalments = n.instalments then 0 else 1 end as instalments
--,case when r.deposit = n.deposit then 0 else 1 end as deposit
--,case when r.userrole = n.userrole then 0 else 1 end as userrole
,case when r.propertymatchpolicy = n.propertymatchpolicy then 0 else 1 end as propertymatchpolicy
,case when r.propertystringpolicy = n.propertystringpolicy then 0 else 1 end as propertystringpolicy
--,case when r.competitorname = n.competitorname then 0 else 1 end as competitorname
--,case when r.competitorpaymenttype = n.competitorpaymenttype then 0 else 1 end as competitorpaymenttype
--,case when r.competitorprice = n.competitorprice then 0 else 1 end as competitorprice
--,case when r.loyaltyreason = n.loyaltyreason then 0 else 1 end as loyaltyreason
--,case when r.onlineoriginal = n.onlineoriginal then 0 else 1 end as onlineoriginal
--,case when r.onlinecurrent = n.onlinecurrent then 0 else 1 end as onlinecurrent
,case when r.covercode = n.covercode then 0 else 1 end as covercode
,case when r.coverperiod = n.coverperiod then 0 else 1 end as coverperiod
,case when r.volxsamt = n.volxsamt then 0 else 1 end as volxsamt
--,case when r.polarisgrantedvoluntaryexcess = n.polarisgrantedvoluntaryexcess then 0 else 1 end as polarisgrantedvoluntaryexcess
,case when r.classofuse = n.classofuse then 0 else 1 end as classofuse
,case when r.driversallowed = n.driversallowed then 0 else 1 end as driversallowed
,case when r.annualmileage = n.annualmileage then 0 else 1 end as annualmileage
,case when r.ownership = n.ownership then 0 else 1 end as ownership
,case when r.keeper = n.keeper then 0 else 1 end as keeper
,case when replace(r.postcodefull_risk,' ','') = replace(n.postcodefull_risk,' ','') then 0 else 1 end as postcodefull_risk
,case when replace(r.postcodesector_risk,' ','') = replace(n.postcodesector_risk,' ','') then 0 else 1 end as postcodesector_risk
,case when r.locationkeptovernight = n.locationkeptovernight then 0 else 1 end as locationkeptovernight
,case when r.purchasedate = n.purchasedate then 0 else 1 end as purchasedate
,case when r.firstregdyear = n.firstregdyear then 0 else 1 end as firstregdyear
--,case when r.vehicleage = n.vehicleage then 0 else 1 end as vehicleage
--,case when r.ownedyears = n.ownedyears then 0 else 1 end as ownedyears
--,case when r.ownedmonths = n.ownedmonths then 0 else 1 end as ownedmonths
,case when r.value = n.value then 0 else 1 end as value
,case when upper(r.manufacturer) = upper(n.manufacturer) then 0 else 1 end as manufacturer
,case when r.model = n.model then 0 else 1 end as model
,case when r.bodytype = n.bodytype then 0 else 1 end as bodytype
,case when r.noofseats = n.noofseats then 0 else 1 end as noofseats
,case when r.cubiccapacity = n.cubiccapacity then 0 else 1 end as cubiccapacity
,case when r.fueltype = n.fueltype then 0 else 1 end as fueltype
,case when r.transmissiontype = n.transmissiontype then 0 else 1 end as transmissiontype
,case when r.leftorrighthanddrive = n.leftorrighthanddrive then 0 else 1 end as leftorrighthanddrive
--,case when r.fittedaeb = n.fittedaeb then 0 else 1 end as fittedaeb
,case when r.modifiedind = n.modifiedind then 0 else 1 end as modifiedind
,case when r.personalimportind = n.personalimportind then 0 else 1 end as personalimportind
,case when r.alarmimmobiliser = n.alarmimmobiliser then 0 else 1 end as alarmimmobiliser
,case when r.trackerdevicefittedind = n.trackerdevicefittedind then 0 else 1 end as trackerdevicefittedind
,case when r.claimedyears = n.claimedyears then 0 else 1 end as claimedyears
,case when r.claimedentitlementreason = n.claimedentitlementreason then 0 else 1 end as claimedentitlementreason
,case when r.claimedyearsearned = n.claimedyearsearned then 0 else 1 end as claimedyearsearned
,case when r.claimedprotectionreqdind = n.claimedprotectionreqdind then 0 else 1 end as claimedprotectionreqdind
,case when r.hpivfs = n.hpivfs then 0 else 1 end as hpivfs
,case when r.hpihri = n.hpihri then 0 else 1 end as hpihri
,case when r.hpikeepers = n.hpikeepers then 0 else 1 end as hpikeepers
--,case when r.hpiowned = n.hpiowned then 0 else 1 end as hpiowned
--,case when r.creditmatch = n.creditmatch then 0 else 1 end as creditmatch
--,case when r.creditqcb = n.creditqcb then 0 else 1 end as creditqcb
,case when r.creditscore = n.creditscore then 0 else 1 end as creditscore
,case when r.idscore = n.idscore then 0 else 1 end as idscore
--,case when r.idscorepanel = n.idscorepanel then 0 else 1 end as idscorepanel
--,case when r.firstseendate = n.firstseendate then 0 else 1 end as firstseendate
--,case when r.seentoinception = n.seentoinception then 0 else 1 end as seentoinception
--,case when r.seentocurrent = n.seentocurrent then 0 else 1 end as seentocurrent
,case when r.propertymatchrisk = n.propertymatchrisk then 0 else 1 end as propertymatchrisk
,case when r.propertystringrisk = n.propertystringrisk then 0 else 1 end as propertystringrisk
--,case when r.valuationdate = n.valuationdate then 0 else 1 end as valuationdate
--,case when r.valuationstring = n.valuationstring then 0 else 1 end as valuationstring
from demo_db.public.gipp_mon_subs g
inner join earnix_rn_base r
on g.rn_submission = r.rn_submission
inner join earnix_nb_base n
on g.nb_submission = n.nb_submission
;
            """
    auto_thirteen = """
    select * from demo_db.public.gipp_mon_subs g
inner join earnix_rn_base r
on g.rn_submission = r.rn_submission
inner join earnix_nb_base n
on g.nb_submission = n.nb_submission
;
            """

    auto_fourteen = """
create or replace temporary table check_driver as
select g.*
,r.driverprn
,case when n.driverprn is not null then 0 else 1 end as driver
,case when r.dateofbirth = n.dateofbirth then 0 else 1 end as dateofbirth
,case when r.age = n.age then 0 else 1 end as age
,case when r.maritalstatus = n.maritalstatus then 0 else 1 end as maritalstatus
,case when r.ukresidencydate = n.ukresidencydate then 0 else 1 end as ukresidencydate
,case when r.ukresidentyears = n.ukresidentyears then 0 else 1 end as ukresidentyears
,case when r.ukresidentfrombirth = n.ukresidentfrombirth then 0 else 1 end as ukresidentfrombirth
,case when r.nonmotorconvictions = n.nonmotorconvictions then 0 else 1 end as nonmotorconvictions
,case when r.accessothervehicles = n.accessothervehicles then 0 else 1 end as accessothervehicles
,case when r.licenceyears = n.licenceyears then 0 else 1 end as licenceyears
,case when r.licencemonths = n.licencemonths then 0 else 1 end as licencemonths
,case when r.licencetype = n.licencetype then 0 else 1 end as licencetype
,case when r.mylicenceind = n.mylicenceind then 0 else 1 end as mylicenceind
,case when r.mylicenceresult = n.mylicenceresult or (r.mylicenceind = n.mylicenceind and r.mylicenceind = 'N') then 0 else 1 end as mylicenceresult
,case when r.passplusind = n.passplusind then 0 else 1 end as passplusind
,case when r.medicalcondition = n.medicalcondition then 0 else 1 end as medicalcondition
--,case when r.ratedoccupation = n.ratedoccupation then 0 else 1 end as ratedoccupation
--,case when r.demeritpoints = n.demeritpoints then 0 else 1 end as demeritpoints
,case when r.employersbusiness_full = n.employersbusiness_full then 0 else 1 end as employersbusiness_full
,case when r.occupationcode_full = n.occupationcode_full then 0 else 1 end as occupationcode_full
,case when r.employmenttype_full = n.employmenttype_full then 0 else 1 end as employmenttype_full
,case when r.employersbusiness_part = n.employersbusiness_part then 0 else 1 end as employersbusiness_part
,case when r.occupationcode_part = n.occupationcode_part then 0 else 1 end as occupationcode_part
,case when r.employmenttype_part = n.employmenttype_part then 0 else 1 end as employmenttype_part
,case when r.relationshiptoproposer = n.relationshiptoproposer then 0 else 1 end as relationshiptoproposer
,case when r.drivesvehicle = n.drivesvehicle then 0 else 1 end as drivesvehicle
from demo_db.public.gipp_mon_subs g
inner join earnix_rn_driver r
on g.rn_submission = r.rn_submission
left join earnix_nb_driver n
on g.nb_submission = n.nb_submission
and r.driverprn = n.driverprn
;

                """
    auto_fifteen = """
create or replace temporary table check_claim as
select g.*
,r.driverprn
,r.type
,r.date
,r.cost
,r.fault
,n.cost as cost_nb
,case when n.type is not null then 0 else 1 end as claim_match
,case when n.type is not null and floor(cast(nvl(r.cost,'0') as numeric(10,2))) = floor(cast(nvl(n.cost,'0') as numeric(10,2))) then 0 else 1 end as cost_match
from demo_db.public.gipp_mon_subs g
inner join earnix_rn_claim r
on g.rn_submission = r.rn_submission
left join earnix_nb_claim n
on g.nb_submission = n.nb_submission
and r.driverprn = n.driverprn
and r.type = n.type
and r.date = n.date
and r.fault = n.fault
;
                    """

    auto_sixteen = """
create or replace temporary table check_conviction as
select g.*
,r.driverprn
,case when r.code = n.code then 0 else 1 end as code
,case when r.date = n.date then 0 else 1 end as date
from demo_db.public.gipp_mon_subs g
inner join earnix_rn_conviction r
on g.rn_submission = r.rn_submission
left join earnix_nb_conviction n
on g.nb_submission = n.nb_submission
and r.driverprn = n.driverprn
and r.conv_no = n.conv_no
;
                    """

    auto_seventeen = """
    create or replace temporary table check_eci as
select g.*
,case when r.daystoinception = dti.value then 0 else 1 end as daystoinception
,case when r.cuepiscore = cup.value then 0 else 1 end as cuepiscore
,case when r.cuescore = nvl(cus.value,'99') then 0 else 1 end as cuescore
,case when (cast(r.apr as numeric (4,2)) > 0 and pt.value = 'true')
           or (cast(r.apr as numeric (4,2)) = 0 and pt.value = 'false') then 0 else 1 end as paymenttype
,case when r.instalmentsrequestedind = dd.value then 0 else 1 end as instalmentsrequestedind
from demo_db.public.gipp_mon_subs g
inner join earnix_rn_base r
on g.rn_submission = r.rn_submission
left join earnix_nb_eci dti
on g.nb_submission = dti.nb_submission
and dti.key = 'DTI'
left join earnix_nb_eci cup
on g.nb_submission = cup.nb_submission
and cup.key = 'CUEPIScore'
left join earnix_nb_eci cus
on g.nb_submission = cus.nb_submission
and cus.key = 'CUEScore'
left join earnix_nb_eci dd
on g.nb_submission = dd.nb_submission
and dd.key = 'InstalmentsRequestedInd'
left join earnix_nb_eci pt
on g.nb_submission = pt.nb_submission
and pt.key = 'InvitedPayment'
;
                       """

    auto_eighteen = """
    -- Summarise
create or replace temporary table check_summary as
with base_cte as (
select rn_submission
      ,nb_submission
      ,rn_date_created
      ,nb_date_created
      ,sum(pricingrandomid+currentdatetime+effectivedate) as date_issue
      ,sum(originalchanneltype+originalchannelcode) as channel_issue
      ,sum(dateofbirth+age+maritalstatus+postcodefull+postcodesector
           +homeownerind+noofchildren+noofvehiclesavailabletofamily
           +insurancerefused) as proposer_issue
      ,sum(propertymatchpolicy+propertystringpolicy+propertymatchrisk+propertystringrisk) as propdb_issue
      ,sum(covercode+coverperiod+volxsamt+classofuse+driversallowed+annualmileage) as cover_issue
      ,sum(ownership+keeper+postcodefull_risk+postcodesector_risk+locationkeptovernight
           +purchasedate+firstregdyear+value+manufacturer+model+bodytype+noofseats
           +cubiccapacity+fueltype+transmissiontype+leftorrighthanddrive+modifiedind
           +personalimportind+alarmimmobiliser+trackerdevicefittedind) as vehicle_issue
      ,sum(claimedyears+claimedentitlementreason+claimedyearsearned+claimedprotectionreqdind) as ncd_issue
      ,sum(creditscore+idscore) as credit_issue
      ,sum(hpivfs+hpihri+hpikeepers) as hpi_issue
from check_base
group by rn_submission, nb_submission, rn_date_created, nb_date_created
),

driver_rn_cnt as (
select rn_submission
      ,count(driverprn) as driver_count
from earnix_rn_driver
group by rn_submission
),

driver_nb_cnt as (
select nb_submission
      ,count(driverprn) as driver_count
from earnix_nb_driver
group by nb_submission
),

driver_cte as (
select rn_submission
      ,nb_submission
      ,sum(dateofbirth+age+maritalstatus+ukresidencydate+ukresidentyears
           +ukresidentfrombirth+nonmotorconvictions+accessothervehicles
           +medicalcondition+relationshiptoproposer+drivesvehicle) as driver_issue
      ,sum(licenceyears+licencemonths+licencetype+mylicenceind+mylicenceresult+passplusind) as licence_issue
      ,sum(employersbusiness_full+occupationcode_full+employmenttype_full) as full_emp_issue
      ,sum(employersbusiness_part+occupationcode_part+employmenttype_part) as part_emp_issue
from check_driver
group by rn_submission, nb_submission
),

claim_rn_cnt as (
select rn_submission
      ,count(*) as claim_count
from earnix_rn_claim
group by rn_submission
),

claim_nb_cnt as (
select nb_submission
      ,count(*) as claim_count
from earnix_nb_claim
group by nb_submission
),

conv_rn_cnt as (
select rn_submission
      ,count(*) as conv_count
from earnix_rn_conviction
group by rn_submission
),

conv_nb_cnt as (
select nb_submission
      ,count(*) as conv_count
from earnix_nb_conviction
group by nb_submission
),

claim_cte as (
select rn_submission
      ,nb_submission
      ,sum(claim_match) as nb_claim_missing
      ,sum(cost_match) as nb_cost_wrong
from check_claim
group by rn_submission, nb_submission
),

conv_cte as (
select rn_submission
      ,nb_submission
      ,sum(code) as code_wrong
      ,sum(date) as date_wrong
from check_conviction
group by rn_submission, nb_submission
),

eci_cte as (
select rn_submission
      ,nb_submission
      ,sum(daystoinception) as dti_issue
      ,sum(cuepiscore+cuescore) as cue_issue
      ,sum(paymenttype+instalmentsrequestedind) as payment_issue
from check_eci
group by rn_submission, nb_submission
)

select b.*
      ,e.dti_issue
      ,e.cue_issue
      ,e.payment_issue
      ,case when dr.driver_count != dn.driver_count then 1 else 0 end as driver_num_issue
      ,d.driver_issue
      ,d.licence_issue
      ,d.full_emp_issue
      ,d.part_emp_issue
      ,case when nvl(clr.claim_count,0) != nvl(cln.claim_count,0) then 1 else 0 end as claim_num_issue
      ,cl.nb_claim_missing
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
left join eci_cte e
on b.nb_submission = e.nb_submission
;
                       """
    results_fin = """
-- Fuller version to aid debugging
create or replace temporary table check_summary_full as
with driver_rn_cnt as (
select rn_submission
      ,count(driverprn) as driver_count
from earnix_rn_driver
group by rn_submission
),

driver_nb_cnt as (
select nb_submission
      ,count(driverprn) as driver_count
from earnix_nb_driver
group by nb_submission
),

driver_cte as (
select rn_submission
      ,nb_submission
      ,sum(dateofbirth) as driver_dateofbirth
      ,sum(age)as driver_age
      ,sum(maritalstatus) as driver_maritalstatus
      ,sum(ukresidencydate) as ukresidencydate
      ,sum(ukresidentyears) as ukresidentyears
      ,sum(ukresidentfrombirth) as ukresidentfrombirth
      ,sum(nonmotorconvictions) as nonmotorconvictions
      ,sum(accessothervehicles) as accessothervehicles
      ,sum(medicalcondition) as medicalcondition
      ,sum(relationshiptoproposer) as relationshiptoproposer
      ,sum(drivesvehicle) as drivesvehicle
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
from check_driver
group by rn_submission, nb_submission
),

claim_rn_cnt as (
select rn_submission
      ,count(*) as claim_count
from earnix_rn_claim
group by rn_submission
),

claim_nb_cnt as (
select nb_submission
      ,count(*) as claim_count
from earnix_nb_claim
group by nb_submission
),

conv_rn_cnt as (
select rn_submission
      ,count(*) as conv_count
from earnix_rn_conviction
group by rn_submission
),

conv_nb_cnt as (
select nb_submission
      ,count(*) as conv_count
from earnix_nb_conviction
group by nb_submission
),

claim_cte as (
select rn_submission
      ,nb_submission
      ,sum(claim_match) as nb_claim_missing
      ,sum(cost_match) as nb_cost_wrong
from check_claim
group by rn_submission, nb_submission
),

conv_cte as (
select rn_submission
      ,nb_submission
      ,sum(code) as code_wrong
      ,sum(date) as date_wrong
from check_conviction
group by rn_submission, nb_submission
),

eci_cte as (
select rn_submission
      ,nb_submission
      ,sum(daystoinception) as dti_issue
      ,sum(cuepiscore+cuescore) as cue_issue
      ,sum(paymenttype+instalmentsrequestedind) as payment_issue
from check_eci
group by rn_submission, nb_submission
)

select b.*
      ,e.dti_issue
      ,e.cue_issue
      ,e.payment_issue
      ,case when dr.driver_count != dn.driver_count then 1 else 0 end as driver_num_issue
      ,d.driver_dateofbirth
      ,d.driver_age
      ,d.driver_maritalstatus
      ,d.ukresidencydate
      ,d.ukresidentyears
      ,d.ukresidentfrombirth
      ,d.nonmotorconvictions
      ,d.accessothervehicles
      ,d.medicalcondition
      ,d.relationshiptoproposer
      ,d.drivesvehicle
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
from check_base b
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
left join eci_cte e
on b.nb_submission = e.nb_submission
;
        """
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
    cs.execute(results_fin)
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
