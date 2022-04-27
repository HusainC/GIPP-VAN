def get_setup1(set_renewal_start_date, set_renewal_end_date):
    refs_sql = f"""
create temporary table pol_sold as
select dpo.skey__ as policy_key
      ,dpo.policy_number
      ,dpr.lineofbusiness_id as lob
      ,dpr.brandcode as brand
      ,dpr.nk_scheme as scheme
      ,dpo.broker_tenure
      ,dpo.cover_end_date_key
      ,edt.date as renewal_date
      ,fsq.submissionnumber as lasttxn_quote_reference
      ,fsq.quote_datekey as lasttxn_datekey
      ,fsq.quote_timekey as lasttxn_timekey
      ,qdt.date as lasttxn_date
      ,fsq.nk_quote_timestamp as lasttxn_timestamp
      ,dtc.transactioncontexttype as lasttxn_type
      ,dtc.transactionreason as lasttxn_reason
      ,row_number() over(partition by dpo.policy_number order by fsq.nk_quote_timestamp desc) as line_number
from edw_dm.dbo.dimpolicy dpo
inner join edw_dm.dbo.fctsorquote fsq
on dpo.skey__ = fsq.policy_key
inner join edw_dm.dbo.dimcalendardate qdt
on fsq.quote_datekey = qdt.skey__
inner join edw_dm.dbo.dimcalendardate edt
on dpo.cover_end_date_key = edt.skey__
inner join edw_dm.dbo.dimtransactioncontext dtc
on fsq.transactioncontext_key = dtc.skey__
inner join edw_dm.dbo.dimproduct dpr
on fsq.product_key = dpr.skey__
where
-- Set renewal date range
edt.date >= '{set_renewal_start_date}' and edt.date <= '{set_renewal_end_date}'
-- Set LoB 'PC','CV','MC','HH'
and dpr.lineofbusiness_id in ('CV')
-- Limit to bound non-temporary transactions
and fsq.sale_flag = 1
and dtc.nk_sourcesystem = 'PC'
and dtc.transactionreason_code not in ('Temp_Change','DeceasedPH','TemporaryDriver','TemporaryVehicle')
;      """

    return refs_sql


def get_setup2():
    refs_sql = f"""
create temporary table pol_live as
with cte as (
select p.*
      ,dtc.transactioncontexttype_code
      ,row_number() over(partition by p.policy_number order by fpt.transactiontimestamp desc) as txn_number
from pol_sold p
inner join edw_dm.dbo.fctpolicytransaction fpt
on p.policy_key = fpt.policy_key
inner join edw_dm.dbo.dimtransactioncontext dtc
on fpt.transactioncontext_key = dtc.skey__
where p.line_number = 1 and dtc.nk_sourcesystem = 'PC'
)
select *
from cte
where txn_number = 1
and transactioncontexttype_code != 'Cancellation'
;       """

    return refs_sql


def get_setup3():
    refs_sql = f"""
create temporary table invites as
with cte as (
select p.*
      ,dpo.skey__ as invite_policy_key
      ,dpr.nk_scheme as invite_scheme
      ,dpr.brandcode as invite_brand
      ,fsq.submissionnumber as invite_quote_reference
      ,fsq.quote_datekey as invite_datekey
      ,fsq.quote_timekey as invite_timekey
      ,qdt.date as invite_date
      ,fsq.nk_quote_timestamp as invite_timestamp
      ,row_number() over(partition by dpo.policy_number order by dpo.skey__ asc, fsq.nk_quote_timestamp asc) as invite_number
from pol_live p
inner join edw_dm.dbo.dimpolicy dpo
on p.policy_number = dpo.policy_number
and p.cover_end_date_key = dpo.cover_start_date_key
inner join edw_dm.dbo.fctsorquote fsq
on dpo.skey__ = fsq.policy_key
inner join edw_dm.dbo.dimcalendardate qdt
on fsq.quote_datekey = qdt.skey__
inner join edw_dm.dbo.dimtransactioncontext dtc
on fsq.transactioncontext_key = dtc.skey__
inner join edw_dm.dbo.dimproduct dpr
on fsq.product_key = dpr.skey__
where
-- Limit to renewal invites including auto-rebroke (but not manual ones)
dtc.business_subtype in ('Renewal Invite (Rebroke)','Renewal Invite (Non Rebroke)')
and dtc.transactioncontexttype = 'Renewal'
)
select * from cte where invite_number = 1
;      """

    return refs_sql


def get_setup4(set_invite_start_date, set_invite_end_date):
    refs_sql = f"""
create temporary table gipp_base as
select i.policy_key
      ,i.policy_number
      ,i.lob
      ,i.brand
      ,i.scheme
      ,i.broker_tenure
      ,i.cover_end_date_key
      ,i.renewal_date
      ,i.lasttxn_quote_reference
      ,i.lasttxn_datekey
      ,i.lasttxn_timekey
      ,i.lasttxn_date
      ,i.lasttxn_timestamp
      ,i.lasttxn_type
      ,i.lasttxn_reason
      ,i.invite_policy_key
      ,i.invite_scheme
      ,i.invite_brand
      ,i.invite_quote_reference
      ,i.invite_datekey
      ,i.invite_timekey
      ,date(i.invite_date)
      ,i.invite_timestamp    
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
from invites i
inner join edw_dm.dbo.fctsorquote fsq
on i.invite_policy_key = fsq.policy_key
and i.invite_datekey = fsq.quote_datekey
and i.invite_timekey = fsq.quote_timekey
inner join edw_dm.dbo.dimtransactioncontext dtc
on fsq.transactioncontext_key = dtc.skey__
inner join edw_dm.dbo.dimcosttype dct
on fsq.costtype_key = dct.skey__
where dtc.business_subtype in ('Renewal Invite (Rebroke)','Renewal Invite (Non Rebroke)')
and dtc.transactioncontexttype = 'Renewal' and date >= '{set_invite_start_date}' and date <='{set_invite_end_date}'
group by i.policy_key
      ,i.policy_number
      ,i.lob
      ,i.brand
      ,i.scheme
      ,i.broker_tenure
      ,i.cover_end_date_key
      ,i.renewal_date
      ,i.lasttxn_quote_reference
      ,i.lasttxn_datekey
      ,i.lasttxn_timekey
      ,i.lasttxn_date
      ,i.lasttxn_timestamp
      ,i.lasttxn_type
      ,i.lasttxn_reason
      ,i.invite_policy_key
      ,i.invite_scheme
      ,i.invite_brand
      ,i.invite_quote_reference
      ,i.invite_datekey
      ,i.invite_timekey
      ,i.invite_date
      ,i.invite_timestamp
;       """

    return refs_sql
