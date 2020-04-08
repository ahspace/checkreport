/*
** SCRIPT SQL : IN_CUSTOMER_EXT-1.1.6.sql
** VERSION    : 1.1.6
** DATE       : 02/08/2018
** MODIFIED DATE: 15/10/2019
** DESCRIPTION:  Change regarding Performance improvement
*       Uses bind variables, instead of substitution variables => stable/same SQL_ID
*       Uses raw data (no temporary tables were used) => it can be executed at any moment without waiting for temporary tables to finish
*       Includes Marie updates
*       Includes full mode script
19/11/2019 : Remove inactive users in fullactive mode
27/11/2019 : Adapt CompiV1.2 changes
             Added grade_name as CUST_SPH_26																		
*/
with
mv_txn_header_as_of_date as (
select /*+ materialize */
    trunc(modified_on) as_of_date,
    created_on,
	transfer_date,
	transfer_id,
    transfer_subtype,
	transfer_status,
	reconciliation_by,
	service_type,
	attr_2_name,
	attr_2_value,
    reference_number
from mtx_transaction_header
where transfer_date < to_date(:TS_STARTD,'DD/MM/YYYY HH24:MI:SS')
    and modified_on >= to_date(:TS_STARTD,'DD/MM/YYYY HH24:MI:SS')
    and modified_on < to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS')
)
, mv_mtx_transaction_items as (
select
      transfer_id
    , unreg_user_id
from mtx_transaction_items ti
where transfer_date >= to_date(:TS_STARTD,'DD/MM/YYYY HH24:MI:SS')
    and transfer_date < to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS')
    and transaction_type = 'MR'
)
, mv_users_data as (
select /*+ FULL(mp) */
    decode(mw.user_type,'OPERATOR', mw.user_type, nvl(mp.user_type, u.user_type)) user_type
    , nvl2(mp.user_id, mp.msisdn, nvl2(u.user_id, u.msisdn, mw.msisdn)) msisdn
    , nvl(mp.user_name, u.user_name) user_name
    , nvl(mp.last_name, u.last_name) last_name
    , nvl(mp.address1, u.address1) address1
    , nvl(mp.city, u.city) city
    , case
        when regexp_like(mp.external_code, '^1[[:upper:]]{5}.+') and substr(mp.external_code,5,2)<> 'XX' then substr(mp.external_code,5,2)
        else upper(mp.RESIDENCE_COUNTRY)
    end addon_homecountry
    , case
        when regexp_like(mp.external_code, '^1[[:upper:]]{5}.+') and substr(mp.external_code,3,2)<> 'XX' then substr(mp.external_code,3,2)
        else upper(mp.NATIONALITY)
    end addon_nationality
    , upper(trim(nvl2(mp.user_id, mp.state, nvl2(u.user_id, u.designation, mw.msisdn)))) user_mark
    , trunc(nvl(mp.dob, u.dob),'DD') dob
    , nvl2(mp.user_id, mp.created_on, nvl2(u.user_id, u.created_on, mw.created_on)) created_on
    , u.agent_code
    , nvl(mp.gender, u.gender) gender
    , decode(mw.user_type, 'OPERATOR', mw.status, nvl(mp.status, u.status)) status
    , mw.is_primary wallet_primary
    , mw.status wallet_status
    , trunc(nvl2(mp.user_id, mp.created_on, nvl2(u.user_id, u.level2_approved_on, mw.created_on)),'DD') creation_date
    , trunc(nvl2(mp.user_id, mp.modified_on, nvl2(u.user_id, u.modified_approved_on, mw.modified_on)),'DD') modification_date
    , nvl(nvl(mw.USER_ID,mp.user_id),u.user_id) as User_Id
    ,nvl(nvl(mw.CREATED_BY,mp.CREATED_BY),u.CREATED_BY) as Created_by
    ,head_mc.category_name as HEAD_CATEGORY_NAME
    ,head_mcr.from_category as head_category_code
    ,mc.sequence_no as hierarchical_level
    ,mp.remarks
    ,mp.ret_msisdn
    ,u.bulk_id
    ,nvl(u.batch_id, mp.batch_id) as BATCH_ID
    ,mc.category_name
    ,NVL(u.CATEGORY_CODE, mp.CATEGORY_CODE) as CATEGORY_CODE
    ,upper(trim(nvl(mp.state,u.state))) as state
  ,case when REGEXP_LIKE(mp.external_code, '^1[[:upper:]]{5}.+') then
                   (select idtype_label from dbref_idtypes where type_id = substr(mp.external_code,2,1))
     else nvl(mp.id_type,'OTHER')
                 end as addon_idtype
     ,replace(replace(convert(mp.id_no, 'US7ASCII', 'WE8ISO8859P1'),chr(10),' '),chr(13),' ') as ID_NO
     ,case when REGEXP_LIKE(nvl(mp.external_code, u.external_code), '^1[[:upper:]]{5}.+') then
				substr(mp.external_code,7)
        else nvl(ID_NO, replace(replace(convert(nvl(mp.external_code,u.external_code), 'US7ASCII', 'WE8ISO8859P1'),chr(10),' '),chr(13),' ') )
                 end as external_code
     ,nvl(mp.address2,u.address2) as address2
         , nvl(u.network_code, mp.network_code) as network_code
         ,mw.wallet_number
         , cg.grade_name
		 ,mw.modified_on as wallet_modified_on								  
from mtx_wallet mw
    left join mtx_party mp on (mw.user_id = mp.user_id and mw.user_type = mp.user_type)
    left join users u on (mw.user_id = u.user_id and mw.user_type <> 'SUBSCRIBER')
    left join mtx_category_relations head_mcr on (head_mcr.to_category =nvl(mp.category_code, u.category_code)) and head_mcr.relation_type ='OWNER' and head_mcr.status='Y'
    left join mtx_categories mc on mc.category_code= nvl(mp.category_code,u.category_code)
    left join mtx_categories head_mc on head_mcr.from_category = head_mc.category_code
    left join channel_grades cg on (mw.user_grade = cg.grade_code and cg.status = 'Y')
where mw.user_type = 'OPERATOR' or mp.user_id is not null or u.user_id is not null
)
,
/*list of actives users the last 91 Days before the start date */
list_actives_91_Days AS(

        SELECT /*+ FULL(ti) */ distinct ti.party_id, ti.ACCOUNT_ID msisdn
    FROM MTX_TRANSACTION_ITEMS ti
        inner join SYS_SERVICE_TYPES s on s.service_type = ti.service_type AND s.IS_FINANCIAL = 'Y'
        inner join MTX_CATEGORIES c on c.CATEGORY_CODE = ti.CATEGORY_CODE and c.CATEGORY_TYPE in ( 'CHUSER', 'SUBS')
    WHERE
        :TYPOFFILE = 'DELTA'
                AND ((ti.transaction_type = 'MR')
                        OR      (ti.transaction_type = 'MP'))
                AND ti.transfer_date >= to_date(:TS_STARTD,'DD/MM/YYYY HH24:MI:SS') - 91
                AND ti.transfer_date < to_date(:TS_STARTD,'DD/MM/YYYY HH24:MI:SS')
        AND ti.transfer_status = 'TS'
),

/*list of actives users on the day before extraction */

list_actives_Of_Day AS (
        SELECT /*+ FULL(ti) */ distinct ti.PARTY_ID, ti.ACCOUNT_ID msisdn
    FROM MTX_TRANSACTION_ITEMS ti
        inner join SYS_SERVICE_TYPES s on s.service_type = ti.service_type AND s.IS_FINANCIAL = 'Y'
        inner join MTX_CATEGORIES c on c.CATEGORY_CODE = ti.CATEGORY_CODE and c.CATEGORY_TYPE in ( 'CHUSER', 'SUBS')
        WHERE
        :TYPOFFILE = 'DELTA'
                AND ((ti.transaction_type = 'MR')
                        OR      (ti.transaction_type = 'MP'))
                AND ti.transfer_date >= to_date(:TS_STARTD,'DD/MM/YYYY HH24:MI:SS')
                AND ti.transfer_date < to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS')
                AND ti.transfer_status = 'TS'
),

/* list of actives users on the last 90 days before extraction*/

list_actives_last_90_days AS (
        SELECT /*+ FULL(ti) */ distinct ti.PARTY_ID, ti.ACCOUNT_ID msisdn
    FROM MTX_TRANSACTION_ITEMS ti
        inner join SYS_SERVICE_TYPES s on s.service_type = ti.service_type AND s.IS_FINANCIAL = 'Y'
        inner join MTX_CATEGORIES c on c.CATEGORY_CODE = ti.CATEGORY_CODE and c.CATEGORY_TYPE in ( 'CHUSER', 'SUBS')
        WHERE
        :TYPOFFILE =  'FULLACTIVE'
                AND ((ti.transaction_type = 'MR')
                        OR      (ti.transaction_type = 'MP'))
                AND ti.transfer_date >= to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS') - 90
                AND ti.transfer_date < to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS')
                AND ti.transfer_status = 'TS'
),

/*For FULL Mode*/
 mv_txn_header as (
select
        case
            when trunc(modified_on) >= trunc(transfer_date) and trunc(modified_on) < to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS') then trunc(modified_on)
            else trunc(transfer_date)
        end as_of_date,
        created_on,
		transfer_date,
		transfer_id,
		transfer_subtype,
		transfer_status,
		reconciliation_by,
		service_type,
		attr_2_name,
		attr_2_value,
    reference_number
from mtx_transaction_header th
where transfer_date >= to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS')-1
    and transfer_date < to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS')
),
 txn as(
 select
 tis.pseudo_user_id sender_pseudo_user_id,
  tis.wallet_number as sender_wallet_number,
  tir.txn_mode,
  tir.wallet_number as receiver_wallet_number,
  tir.party_id as receiver_user_id,
  tis.service_type,
  tis.party_id as sender_user_id,
  tir.pseudo_user_id receiver_pseudo_user_id,
  tir.account_id as receiver_msisdn,
  tis.account_id as sender_msisdn,
  tis.requested_value/nvl((select DEFAULT_VALUE
        from  MTX_SYSTEM_PREFERENCES
        where PREFERENCE_CODE = 'CURRENCY_FACTOR'),100) as transaction_amount,
  decode(tir.transaction_type,'MR',case when tir.service_type in ('ROLLBACK','TXNCORRECT') then decode(tir.party_id,'IND03','CR',decode(tir.second_party,'IND03','SCR',tir.transaction_type))
  else tir.transaction_type end,tir.transaction_type) as transaction_type,
   sum(case
                    when (tir.transaction_type = 'SCR'
                            or (tir.transaction_type = 'MR' and tir.service_type in ('ROLLBACK','TXNCORRECT') and tir.second_party = 'IND03'))
                    then tir.transfer_value/nvl((select DEFAULT_VALUE
        from  MTX_SYSTEM_PREFERENCES
        where PREFERENCE_CODE = 'CURRENCY_FACTOR'),100)
                    else 0
                end) over (partition by tis.transfer_id) as total_sc,
  tis.transfer_id
  from mtx_transaction_items tir
         inner join mtx_transaction_items tis on (tir.transfer_id = tis.transfer_id
             and tir.party_id = tis.second_party
             and tir.second_party = tis.party_id
             and tir.transfer_value = tis.transfer_value)
    where ((tir.transaction_type = 'MR' and tis.transaction_type = 'MP')
             or (tir.transaction_type = 'CR' and tis.transaction_type = 'CP')
             or (tir.transaction_type = 'SCR' and tis.transaction_type = 'SCP'))
            and tir.transfer_date >= to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS')-1
            and tir.transfer_date < to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS')
            and tis.transfer_date >= to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS')-1
            and tis.transfer_date < to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS')

	UNION ALL
	select 
  tisa.pseudo_user_id sender_pseudo_user_id,
	tisa.wallet_number as sender_wallet_number,
	tira.txn_mode,
	tira.wallet_number as receiver_wallet_number,
	tira.party_id as receiver_user_id,
	tisa.service_type,
	tisa.party_id as sender_user_id,
	tira.pseudo_user_id receiver_pseudo_user_id,
	tira.account_id as receiver_msisdn,
	tisa.account_id as sender_msisdn,
  tisa.requested_value/nvl((select DEFAULT_VALUE
        from  MTX_SYSTEM_PREFERENCES
        where PREFERENCE_CODE = 'CURRENCY_FACTOR'),100) as transaction_amount,
  decode(tira.transaction_type,'MR',case when tira.service_type in ('ROLLBACK','TXNCORRECT') then decode(tira.party_id,'IND03','CR',decode(tira.second_party,'IND03','SCR',tira.transaction_type))
  else tira.transaction_type end,tira.transaction_type) as transaction_type,
  sum(case
                    when (tira.transaction_type = 'SCR'
                            or (tira.transaction_type = 'MR' and tira.service_type in ('ROLLBACK','TXNCORRECT') and tira.second_party = 'IND03'))
                    then tira.transfer_value/nvl((select DEFAULT_VALUE
        from  MTX_SYSTEM_PREFERENCES
        where PREFERENCE_CODE = 'CURRENCY_FACTOR'),100)
                    else 0
                end) over (partition by tisa.transfer_id) as total_sc,
  tisa.transfer_id
   from mv_txn_header_as_of_date thid
         inner join mtx_transaction_items tira on (thid.transfer_id = tira.transfer_id)
         inner join mtx_transaction_items tisa on (tira.transfer_id = tisa.transfer_id
             and tira.party_id = tisa.second_party
             and tira.second_party = tisa.party_id
             and tira.transfer_value = tisa.transfer_value)
    where ((tira.transaction_type = 'MR' and tisa.transaction_type = 'MP')
             or (tira.transaction_type = 'CR' and tisa.transaction_type = 'CP')
             or (tira.transaction_type = 'SCR' and tisa.transaction_type = 'SCP'))
),
txn_full as(
select
nvl2(txn.sender_pseudo_user_id, 'PSEUDO', dus.user_type) sender_user_type,
txn.sender_wallet_number,
th.created_on as CREATED_ON_NUM,
th.transfer_date AS transfer_date_num,
th.transfer_id,
    txn.receiver_wallet_number,
    txn.receiver_user_id,
    nvl2(txn.receiver_pseudo_user_id, 'PSEUDO', dur.user_type) receiver_user_type,
    txn.receiver_msisdn,
    txn.sender_msisdn,
    txn.sender_user_id,
    decode(th.transfer_status,'TS','Y',decode(th.reconciliation_by, null, case when th.service_type = 'RC' and th.attr_2_name = 'FAILED_AT_IN' and th.attr_2_value = 'Y' then 'Y' else 'N' end,'Y')) as transfer_done
    ,nvl(txn.transaction_amount,0) as transaction_amount
 from(
select * from mv_txn_header
    union all
    select * from mv_txn_header_as_of_date) th
    inner join txn
	on (th.transfer_id = txn.transfer_id and txn.transaction_type = 'MR')
	inner join sys_service_types sst on (txn.service_type = sst.service_type)
    left join mv_users_data dus on (dus.wallet_number = txn.sender_wallet_number)
    left join mv_users_data dur on (dur.wallet_number = txn.receiver_wallet_number)
	where (sst.is_financial = 'Y' or txn.total_sc > 0)
),

activity_data as(
select nvl(debit_tr.user_id, credit_tr.user_id) as user_id, nvl(debit_tr.user_type, credit_tr.user_type) as user_type,
total_debit, total_credit, max_debit_date, max_credit_date,
	case when nvl(max_credit_date,to_date('01/01/1900','DD/MM/YYYY')) > nvl(max_debit_date,to_date('01/01/1900','DD/MM/YYYY'))
then max_credit_date else max_debit_date end as last_transfer_date,
	case when total_debit >= (select DECODE(NETWORK_CODE,'GC',2500000,200000) P_VAL_NUM FROM NETWORKS) then 'F'
     when total_credit >= (select DECODE(NETWORK_CODE,'GC',2500000,200000) P_VAL_NUM FROM NETWORKS) then 'F'
else 'L' end as activity_volumetry_indicator
from (
(select sender_user_id as user_id, sender_user_type as user_type, sum(transaction_amount) as total_debit, max(TRANSFER_DATE_NUM) as max_debit_date
	from txn_full
	where transfer_done = 'Y'
	and transfer_date_num >= to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS')-30
	group by sender_user_id, sender_user_type)  debit_tr
full outer join
	(select receiver_user_id as user_id, receiver_user_type as user_type, sum(transaction_amount) as total_credit, max(TRANSFER_DATE_NUM) as max_credit_date
	from txn_full
	where transfer_done = 'Y'
	and transfer_date_num >= to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS')-30
	group by receiver_user_id, receiver_user_type)   credit_tr
on debit_tr.user_id = credit_tr.user_id and debit_tr.user_type = credit_tr.user_type)
)
SELECT
'"'||   INSTITUTE       ||      '"|'||
'"'||   CUSTNO          ||      '"|'||
'"'||   CUST_SPH_01     ||      '"|'||
'"'||   CUST_SPH_02     ||      '"|'||
'"'||   CUST_SPH_03     ||      '"|'||
'"'||   CUST_SPH_04     ||      '"|'||
'"'||   CUST_SPH_05     ||      '"|'||
'"'||   CUST_SPH_06     ||      '"|'||
'"'||   CUST_SPH_07     ||      '"|'||
'"'||   CUST_SPH_08     ||      '"|'||
'"'||   CUST_SPH_09     ||      '"|'||
'"'||   CUST_SPH_10     ||      '"|'||
'"'||   CUST_SPH_11     ||      '"|'||
'"'||   CUST_SPH_12     ||      '"|'||
'"'||   CUST_SPH_13     ||      '"|'||
'"'||   CUST_SPH_14     ||      '"|'||
'"'||   CUST_SPH_15     ||      '"|'||
'"'||   CUST_SPH_16     ||      '"|'||
'"'||   CUST_SPH_17     ||      '"|'||
'"'||   CUST_SPH_18     ||      '"|'||
'"'||   CUST_SPH_19     ||      '"|'||
'"'||   CUST_SPH_20     ||      '"|'||
'"'||   CUST_SPH_21     ||      '"|'||
'"'||   CUST_SPH_22     ||      '"|'||
'"'||   CUST_SPH_23     ||      '"|'||
'"'||   CUST_SPH_24     ||      '"|'||
'"'||   CUST_SPH_25     ||      '"|'||
'"'||   CUST_SPH_26     ||      '"|'||
'"'||   CUST_SPH_27     ||      '"|'||
'"'||   CUST_SPH_28     ||      '"|'||
'"'||   CUST_SPH_29     ||      '"|'||
'"'||   CUST_SPH_30     ||      '"|'||
'"'||   CUST_SPH_31     ||      '"|'||
'"'||   CUST_SPH_32     ||      '"|'||
'"'||   CUST_SPH_33     ||      '"|'||
'"'||   CUST_SPH_34     ||      '"|'||
'"'||   CUST_SPH_35     ||      '"|'||
'"'||   CUST_SPH_36     ||      '"|'||
'"'||   CUST_SPH_37     ||      '"|'||
'"'||   CUST_SPH_38     ||      '"|'||
'"'||   CUST_SPH_39     ||      '"|'||
'"'||   CUST_SPH_40     ||      '"|'||
'"'||   CUST_SPH_41     ||      '"|'||
'"'||   CUST_SPH_42     ||      '"|'||
'"'||   CUST_SPH_43     ||      '"|'||
'"'||   CUST_SPH_44     ||      '"|'||
'"'||   CUST_SPH_45     ||      '"|'||
'"'||   CUST_SPH_46     ||      '"|'||
'"'||   CUST_SPH_47     ||      '"|'||
'"'||   CUST_SPH_48     ||      '"|'||
        CUST_NPH_01     ||       '|'||
        CUST_NPH_02     ||       '|'||
        CUST_NPH_03     ||       '|'||
        CUST_NPH_04     ||       '|'||
        CUST_NPH_05     ||       '|'||
        CUST_NPH_06     ||       '|'||
        CUST_NPH_07     ||       '|'||
        CUST_NPH_08     ||       '|'||
        CUST_NPH_09     ||       '|'||
        CUST_NPH_10
AS LINE
                    /*IN_CUSTOMER_EXT_CHA_SUB.sql*/
FROM  (
SELECT Distinct
        RPAD(:INSTITUTE,4)                              institute,
        substr(u.user_type,1,1)||nvl(u.msisdn,' ')              custno,             --
        translate(SUBSTR(u.user_name,33),'"',' ')                               cust_sph_01,        --
        translate(SUBSTR(u.last_name,33),'"',' ')                               cust_sph_02,        --
        translate(SUBSTR(u.address1,33),'"',' ')                        cust_sph_03,        --
        translate(u.address2,'"',' ')                           cust_sph_04,        --
        translate(SUBSTR(u.city,29),'"',' ')                            cust_sph_05,        --
        decode(addon_idtype, 'PASSPORT', '',translate(u.external_code,'"',' '))                  cust_sph_06,        --
        u.id_no                                                 cust_sph_07,        --
        u.addon_idtype                                                  cust_sph_08,        --
        u.category_code                                         cust_sph_09,        --
        u.category_name                                         cust_sph_10,
        decode(u.user_type,'CHANNEL',u.state,null)              cust_sph_11,
        null                                            cust_sph_12,
        null            cust_sph_13,
        null                                    cust_sph_14,
        null                                    cust_sph_15,
        null                            cust_sph_16,
        translate(SUBSTR(u.remarks,1,64),'"',' ')                                       cust_sph_17,
        translate(SUBSTR(u.remarks,65),'"',' ')                                 cust_sph_18,
        u.user_id                                               cust_sph_19,
        u.hierarchical_level                                    cust_sph_20,
        u.head_category_code                                    cust_sph_21,
        u.head_category_name                                    cust_sph_22,
         null                                                   cust_sph_23,

		 null                                                      cust_sph_24,
         null                                                           cust_sph_25,
        u.grade_name                                                            cust_sph_26,
        null                                                            cust_sph_27,
        null                                                            cust_sph_28,
        null                                                            cust_sph_29,
        null                                                            cust_sph_30,
        null                                                            cust_sph_31,
        null                                                            cust_sph_32,
        null                                                            cust_sph_33,
        null                                                            cust_sph_34,
        null                                                            cust_sph_35,
        null                                                            cust_sph_36,
        null                                                            cust_sph_37,
        null                                                            cust_sph_38,
        null                                                            cust_sph_39,
        null                                                            cust_sph_40,
        null                                                            cust_sph_41,
        null                                                            cust_sph_42,
        null                                                            cust_sph_43,
        null                                                            cust_sph_44,
        null                                                            cust_sph_45,
        null                                                            cust_sph_46,
        null                                                            cust_sph_47,
        null                                                            cust_sph_48,
        null                                                  cust_nph_01,
                null                                       cust_nph_02,
                to_number(null)                                         cust_nph_03,
                to_number(null)                                         cust_nph_04,
                to_number(null)                                         cust_nph_05,
                to_number(null)                                         cust_nph_06,
                to_number(null)                                         cust_nph_07,
                to_number(null)                                         cust_nph_08,
                to_number(null)                                         cust_nph_09,
                to_number(null)                                         cust_nph_10
FROM   (
                        SELECT /*+ FULL(u) */
                                u.*
                                FROM mv_users_data u
                                        inner join list_actives_last_90_days on list_actives_last_90_days.PARTY_ID = u.user_id and list_actives_last_90_days.msisdn = u.msisdn
                                        left join imt_country_code i on i.country_iso = u.network_code
                                WHERE
                                        :TYPOFFILE = 'FULLACTIVE'
                                        AND u.user_type in ('SUBSCRIBER','CHANNEL')
                                        AND u.wallet_primary = 'Y'
                                        AND u.wallet_status <> 'N'
                                        AND u.status <> 'N'
                        UNION

                                SELECT /*+ FULL(u) */
                                u.*
                                FROM mv_users_data u
                                        inner join (SELECT L2.PARTY_ID , L2.msisdn
                                                FROM list_actives_91_Days  L1
                                                        RIGHT JOIN list_actives_Of_Day L2 ON (L1.party_id = L2.PARTY_ID and L1.msisdn = L2.msisdn )
                                                WHERE
                                                        L1.party_id is null) List_reactives on List_reactives.PARTY_ID = u.user_id and List_reactives.msisdn = u.msisdn
                                        left join imt_country_code i on i.country_iso = u.network_code
                                WHERE  :TYPOFFILE = 'DELTA'
                                        AND u.status <> 'N'
                                        AND u.user_type in ('SUBSCRIBER','CHANNEL')
                                        AND     u.creation_date < to_date(:TS_STARTD,'DD/MM/YYYY HH24:MI:SS') - 91
                                        AND u.wallet_primary = 'Y'
                                        AND u.wallet_status <> 'N'

                        UNION

                Select /*+ FULL(u) */
                        u.*
                        FROM    mv_users_data u
                        WHERE   u.status <> 'N'
                                AND u.user_type in ('SUBSCRIBER','CHANNEL')
                                AND u.wallet_primary = 'Y'
                                AND u.wallet_status <> 'N'
                                AND     (   (u.creation_date  >=  to_date(:TS_STARTD,'DD/MM/YYYY HH24:MI:SS') AND u.creation_date   <  to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS'))
                                        OR (u.modification_date  >=  to_date(:TS_STARTD,'DD/MM/YYYY HH24:MI:SS') AND u.modification_date  <  to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS'))
										OR (u.wallet_modified_on  >=  to_date(:TS_STARTD,'DD/MM/YYYY HH24:MI:SS') AND u.wallet_modified_on  <  to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS')))
                                and (:TYPOFFILE <> 'FULL' or (u.status <> 'N' and u.wallet_status <> 'N' and u.wallet_primary ='Y') )
                                AND ( U.MSISDN = :MSISDN OR 'ALL' =  :MSISDN )
                ) u
LEFT JOIN activity_data
on u.user_id = activity_data.user_id and u.user_type = activity_data.user_type

UNION ALL            /*IN_CUSTOMER_EXT_TNO.sql*/
SELECT Distinct
        RPAD(:INSTITUTE,4)                              institute,
        'U'||nvl(UU.msisdn,' ')         custno,
        translate(SUBSTR(RK.rec_fname,33),'"',' ')      cust_sph_01,        --
        translate(SUBSTR(RK.rec_lname,33),'"',' ')      cust_sph_02,        --
        null                                                                            cust_sph_03,        --
        null                                                                            cust_sph_04,        --
        null                                                                            cust_sph_05,        --
        null                                                                            cust_sph_06,        --
        RK.rec_idno                                                             cust_sph_07,        --
        null                                                                            cust_sph_08,        --
        null                                                                    cust_sph_09,        --
                null                                                                            cust_sph_10,
                null                                                                            cust_sph_11,
                null                                                                            cust_sph_12,
                null                                                                            cust_sph_13,
                null                                                                            cust_sph_14,
                null                                                                            cust_sph_15,
                null                                                                            cust_sph_16,
                null                                                                            cust_sph_17,
                null                                                                            cust_sph_18,
                uu.unreg_user_id                                                        cust_sph_19,
                null                                                                            cust_sph_20,
                null                                                                            cust_sph_21,
                null                                                                            cust_sph_22,
                null                                                                            cust_sph_23,
                null                                                                            cust_sph_24,
                null                                                                            cust_sph_25,
                null                                                                            cust_sph_26,
                null                                                                            cust_sph_27,
                null                                                                            cust_sph_28,
                null                                                                            cust_sph_29,
                null                                                                            cust_sph_30,
                null                                                                            cust_sph_31,
                null                                                                            cust_sph_32,
                null                                                                            cust_sph_33,
                null                                                                            cust_sph_34,
                null                                                                            cust_sph_35,
                null                                                                            cust_sph_36,
                null                                                                            cust_sph_37,
                null                                                                            cust_sph_38,
                null                                                                            cust_sph_39,
                null                                                                            cust_sph_40,
                null                                                                            cust_sph_41,
                null                                                                            cust_sph_42,
                null                                                                            cust_sph_43,
                null                                                                            cust_sph_44,
                null                                                                            cust_sph_45,
                null                                                                            cust_sph_46,
                null                                                                            cust_sph_47,
                null                                                                            cust_sph_48,
                to_char(null)                                                         cust_nph_01,
                to_char(null)                                                         cust_nph_02,
                to_number(null)                                                         cust_nph_03,
                to_number(null)                                                         cust_nph_04,
                to_number(null)                                                         cust_nph_05,
                to_number(null)                                                         cust_nph_06,
                to_number(null)                                                         cust_nph_07,
                to_number(null)                                                         cust_nph_08,
                to_number(null)                                                         cust_nph_09,
                to_number(null)                                                         cust_nph_10
from recieverkyc rk
        inner join (
            select mv.transfer_id
                , mv.unreg_user_id
            from mv_mtx_transaction_items mv
            union all
            select ti.transfer_id
                , ti.unreg_user_id
            from mtx_transaction_items ti
                join mv_txn_header_as_of_date th on (ti.transfer_id = th.transfer_id)
            where ti.transaction_type = 'MR'
        ) tx on rk.p2p_transfer_id = tx.transfer_id
        inner join mtx_unreg_user uu on uu.unreg_user_id = tx.unreg_user_id
)
ORDER   BY  custno
