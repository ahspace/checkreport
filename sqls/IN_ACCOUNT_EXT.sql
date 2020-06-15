/*
** SCRIPT SQL : IN_ACCOUNT_EXT.sql
** VERSION    : 2.3.3.C1
** DATE       : 10/09/2019
** Modified	  : 30/04/2020
*/
With wallet_txn as(
select 
decode(th.transfer_status,'TS','Y',decode(th.reconciliation_by, null, case when th.service_type = 'RC' and th.attr_2_name = 'FAILED_AT_IN' and th.attr_2_value = 'Y' then 'Y' else 'N' end,'Y')) as transfer_done,
  tis.wallet_number as sender_wallet_number,
  tir.wallet_number as receiver_wallet_number
  from mtx_transaction_header th
  join mtx_transaction_items tir on th.TRANSFER_ID = tir.TRANSFER_ID
         inner join mtx_transaction_items tis on (tir.transfer_id = tis.transfer_id
             and tir.party_id = tis.second_party
             and tir.second_party = tis.party_id
             and tir.transfer_value = tis.transfer_value)
    where (tir.transaction_type = 'MR' and tis.transaction_type = 'MP')
            and tir.transfer_date >= to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS')-1
            and tir.transfer_date < to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS')
            and tis.transfer_date >= to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS')-1
            and tis.transfer_date < to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS')
),
txn as(
select distinct sender_wallet_number, receiver_wallet_number
from wallet_txn where transfer_done = 'Y'
),
mv_wallet as(
select user_type,msisdn,is_primary,status, wallet_number,modified_on,created_on,user_id,
    mpay_profile_id, payment_type_id from mtx_wallet mw
    join txn send_txn on mw.wallet_number = send_txn.sender_wallet_number
    union all
    select user_type,msisdn,is_primary,status, wallet_number,modified_on,created_on,user_id,
    mpay_profile_id, payment_type_id from mtx_wallet mw
    join txn rec_txn on mw.wallet_number = rec_txn.receiver_wallet_number
),
mv_users_data as (
select  distinct
    decode(mw.user_type,'OPERATOR', mw.user_type, nvl(mp.user_type, u.user_type)) user_type
    , nvl2(mp.user_id, mp.msisdn, nvl2(u.user_id, u.msisdn, mw.msisdn)) msisdn
    , decode(mw.user_type, 'OPERATOR', mw.status, nvl(mp.status, u.status)) status
    , mw.is_primary wallet_primary
    , mw.status wallet_status
    , nvl(u.network_code, mp.network_code) as network_code
    , mw.wallet_number
    , mw.modified_on as wallet_modified_on
    , mw.created_on as wallet_created_on
    ,spms.subtype_name as wallet_type_name
    ,mcp.max_balance/nvl((select DEFAULT_VALUE
        from  MTX_SYSTEM_PREFERENCES
        where PREFERENCE_CODE = 'CURRENCY_FACTOR'),100) as wallet_maximum_balance
    ,mw.payment_type_id as wallet_type_id
          ,cg.grade_name
    ,mw.user_id
from
    mv_wallet mw
    left join mtx_party mp on (mw.user_id = mp.user_id and mw.user_type = mp.user_type)
    left join users u on (mw.user_id = u.user_id and mw.user_type <> 'SUBSCRIBER')
    left join mtx_categories mc on (mc.category_code = nvl(mp.category_code, u.category_code))
    left join mtx_trf_cntrl_profile mcp on (mw.mpay_profile_id = mcp.profile_id)
    left join channel_grades cg on (mcp.grade_code = cg.grade_code and cg.status = 'Y')
    left join sys_payment_method_subtypes spms on (mw.payment_type_id = spms.payment_type_id)
where mw.user_type = 'OPERATOR' or mp.user_id is not null or u.user_id is not null
)

SELECT
'"'||   institute       ||              '"|'||
'"'||   custno          ||              '"|'||
'"'||   businesstype    ||              '"|'||
'"'||   accno           ||              '"|'||
'"'||   businessno      ||              '"|'||
'"'||   acc_currencyiso ||              '"|'||
'"'||   acc_sph_01      ||              '"|'||
'"'||   acc_sph_02      ||              '"|'||
'"'||   acc_sph_03      ||              '"|'||
'"'||   acc_sph_04      ||              '"|'||
'"'||   acc_sph_05      ||              '"|'||
'"'||   acc_sph_06      ||              '"|'||
'"'||   acc_sph_07      ||              '"|'||
'"'||   acc_sph_08      ||              '"|'||
'"'||   acc_sph_09      ||              '"|'||
'"'||   acc_sph_10      ||              '"|'||
'"'||   acc_sph_11      ||              '"|'||
'"'||   acc_sph_12      ||              '"|'||
'"'||   acc_sph_13      ||              '"|'||
'"'||   acc_sph_14      ||              '"|'||
'"'||   acc_sph_15      ||              '"|'||
'"'||   acc_sph_16      ||              '"|'||
'"'||   acc_sph_17      ||              '"|'||
'"'||   acc_sph_18      ||              '"|'||
'"'||   acc_sph_19      ||              '"|'||
'"'||   acc_sph_20      ||              '"|'||
'"'||   acc_sph_21      ||              '"|'||
'"'||   acc_sph_22      ||              '"|'||
'"'||   acc_sph_23      ||              '"|'||
'"'||   acc_sph_24      ||              '"|'||
'"'||   acc_sph_25      ||              '"|'||
'"'||   acc_sph_26      ||              '"|'||
'"'||   acc_sph_27      ||              '"|'||
'"'||   acc_sph_28      ||              '"|'||
'"'||   acc_sph_29      ||              '"|'||
'"'||   acc_sph_30      ||              '"|'||
'"'||   acc_sph_31      ||              '"|'||
'"'||   acc_sph_32      ||              '"|'||
'"'||   acc_sph_33      ||              '"|'||
'"'||   acc_sph_34      ||              '"|'||
'"'||   acc_sph_35      ||              '"|'||
'"'||   acc_sph_36      ||              '"|'||
'"'||   acc_sph_37      ||              '"|'||
'"'||   acc_sph_38      ||              '"|'||
'"'||   acc_sph_39      ||              '"|'||
'"'||   acc_sph_40      ||              '"|'||
'"'||   acc_sph_41      ||              '"|'||
'"'||   acc_sph_42      ||              '"|'||
'"'||   acc_sph_43      ||              '"|'||
'"'||   acc_sph_44      ||              '"|'||
'"'||   acc_sph_45      ||              '"|'||
'"'||   acc_sph_46      ||              '"|'||
'"'||   acc_sph_47      ||              '"|'||
'"'||   acc_sph_48      ||              '"|'||
        acc_nph_01      ||               '|'||
        acc_nph_02      ||               '|'||
        acc_nph_03      ||               '|'||
        acc_nph_04      ||               '|'||
        acc_nph_05      ||               '|'||
        acc_nph_06      ||               '|'||
        acc_nph_07      ||               '|'||
        acc_nph_08      ||               '|'||
        acc_nph_09      ||               '|'||
        acc_nph_10
FROM
   (
SELECT
                RPAD(:INSTITUTE,4)                        institute,
       substr(u.user_type,1,1)||nvl(u.msisdn,' ')                                       custno,
       substr(u.wallet_number,1,2)                                                      businesstype,
       substr(u.wallet_number,3,9)                                                      accno,
       substr(u.wallet_number,12,9)                                                     businessno,
       SUBSTR(i.currency_iso,1,3)                                                               acc_currencyiso,
       u.user_id                                                                                                                acc_sph_01,
           u.wallet_type_id                                                                                                     acc_sph_02,
           u.grade_name                                                                                                 acc_sph_03,
                null                                                                    acc_sph_04,
                null                                                                    acc_sph_05,
                null                                                                    acc_sph_06,
                null                                                                    acc_sph_07,
                null                                                                    acc_sph_08,
                null                                                                    acc_sph_09,
                null                                                                    acc_sph_10,
                null                                                                    acc_sph_11,
                null                                                                    acc_sph_12,
                null                                                                    acc_sph_13,
                null                                                                    acc_sph_14,
                null                                                                    acc_sph_15,
                null                                                                    acc_sph_16,
                null                                                                    acc_sph_17,
                null                                                                    acc_sph_18,
                null                                                                    acc_sph_19,
                null                                                                    acc_sph_20,
                null                                                                    acc_sph_21,
                null                                                                    acc_sph_22,
                null                                                                    acc_sph_23,
                null                                                                    acc_sph_24,
                null                                                                    acc_sph_25,
                null                                                                    acc_sph_26,
                null                                                                    acc_sph_27,
                null                                                                    acc_sph_28,
                null                                                                    acc_sph_29,
                null                                                                    acc_sph_30,
                null                                                                    acc_sph_31,
                null                                                                    acc_sph_32,
                null                                                                    acc_sph_33,
                null                                                                    acc_sph_34,
                null                                                                    acc_sph_35,
                null                                                                    acc_sph_36,
                null                                                                    acc_sph_37,
                null                                                                    acc_sph_38,
                null                                                                    acc_sph_39,
                null                                                                    acc_sph_40,
                null                                                                    acc_sph_41,
                null                                                                    acc_sph_42,
                null                                                                    acc_sph_43,
                null                                                                    acc_sph_44,
                null                                                                    acc_sph_45,
                null                                                                    acc_sph_46,
                null                                                                    acc_sph_47,
                null                                                                    acc_sph_48,
                to_number(null)                                                 acc_nph_01,
                to_number(null)                                                 acc_nph_02,
                to_number(null)                                                 acc_nph_03,
                to_number(null)                                                 acc_nph_04,
                to_number(null)                                                 acc_nph_05,
                to_number(null)                                                 acc_nph_06,
                to_number(null)                                                 acc_nph_07,
                to_number(null)                                                 acc_nph_08,
                to_number(null)                                                 acc_nph_09,
                to_number(null)                                                 acc_nph_10
                FROM
                mv_users_data u
                left join imt_country_code i on i.country_iso = u.network_code
                                where u.user_type in ('CHANNEL','SUBSCRIBER')
            and u.status <> 'N' and u.wallet_status <> 'N'
                                                AND ( U.MSISDN = :MSISDN OR 'ALL' =  :MSISDN )
                ORDER   BY
            custno,
            businesstype,
            accno,
            businessno,
            acc_currencyiso
)

