/*
** SCRIPT SQL : IN_ACCOUNT.sql
** VERSION    : 2.3.3.C1
** DATE       : 11/03/2019
** MODIFIED   : 30/04/2020

*/
With wallet_txn as(
select decode(th.transfer_status,'TS','Y',decode(th.reconciliation_by, null, case when th.service_type = 'RC' and th.attr_2_name = 'FAILED_AT_IN' and th.attr_2_value = 'Y' then 'Y' else 'N' end,'Y')) as transfer_done,
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
    , mwb.balance/nvl((select DEFAULT_VALUE
        from  MTX_SYSTEM_PREFERENCES
        where PREFERENCE_CODE = 'CURRENCY_FACTOR'),100) as balance
from
    mv_wallet mw
    join MTX_WALLET_BALANCES mwb on mw.WALLET_NUMBER = mwb.WALLET_NUMBER and mwb.WALLET_SEQUENCE_NUMBER = '0'
    left join mtx_party mp on (mw.user_id = mp.user_id and mw.user_type = mp.user_type)
    left join users u on (mw.user_id = u.user_id and mw.user_type <> 'SUBSCRIBER')
    left join mtx_categories mc on (mc.category_code = nvl(mp.category_code, u.category_code))
    left join mtx_trf_cntrl_profile mcp on (mw.mpay_profile_id = mcp.profile_id)
    left join channel_grades cg on (mcp.grade_code = cg.grade_code and cg.status = 'Y')
    left join sys_payment_method_subtypes spms on (mw.payment_type_id = spms.payment_type_id)
where mw.user_type = 'OPERATOR' or mp.user_id is not null or u.user_id is not null
)
SELECT
INSTITUTE       ||
CUSTNO  ||
BUSINESSTYPE    ||
ACCNO   ||
BUSINESSNO      ||
ACC_CURRENCYISO ||
ACCOPENING      ||
ACCCLOSE        ||
ACCHOLD_INSTITUTE       ||
ACCHOLDCUSTNO   ||
ACCLIMIT        ||
ACCBALANCE      ||
SUMCREDRUNYEAR  ||
SUMDEBRUNYEAR   ||
NUMBERACCOUNTS  ||
CUST_FLAGS      ||
PODTYPE ||
IBAN    ||
ACC_TYPE        ||
HOLD_MAIL       ||
EMPLNO  ||
PURPOSE ||
CONTR_DATINCEPT ||
CONTR_ENDDATE   ||
TARIFF  ||
PAYM_PERIOD     ||
PAYM_MODE       ||
PAYM_TYPE       ||
TYPE_ACQUIS     ||
LIFE_INCREASEYN ||
LIFE_LENTYN     ||
CONTR_STATUS    ||
INTERMED_TYPE   ||
FK_CSMNO        ||
BIC_PREMPAYOR
FROM
   (
SELECT
   RPAD(:INSTITUTE,4)                                           institute,
       RPAD(substr(u.user_type,1,1)||nvl(u.msisdn,' '),16)                                      custno,
       RPAD(substr(u.wallet_number,1,2),4)                                                      businesstype,
       RPAD(substr(u.wallet_number,3,9),11)                                                     accno,
       RPAD(substr(u.wallet_number,12,9),11)                                                    businessno,
       RPAD(i.currency_iso,3)                                                                                           acc_currencyiso,
       TO_CHAR(u.wallet_created_on,'YYYYMMDD')                                                                          accopening,
       RPAD(decode(u.wallet_status,'N',TO_CHAR(u.wallet_modified_on,'YYYYMMDD'),' '),8)         accclose,
       RPAD(' ',4)                                                                                                      acchold_institute,
       RPAD(' ',16)                                                                                                     accholdcustno,
       TO_CHAR(u.wallet_maximum_balance,'S0000000000000.00')                                            acclimit,
       TO_CHAR(u.balance,'S0000000000000.00')                                                                           accbalance,
       RPAD(' ',17)                                                                                                                     sumcredrunyear,
       RPAD(' ',17)                                                                                                                     sumdebrunyear,
       RPAD(' ',4)                                                                                                      numberaccounts,
       RPAD(' ',24)                                                                                             cust_flags,
       RPAD(' ',5)                                                                                                      podtype,
       RPAD(' ',35)                                                                                                     iban,
       ' '                                                                                                              acc_type,
       ' '                                                                                                              hold_mail,
       RPAD(' ',16)                                                                                                     emplno,
       RPAD(u.wallet_type_name,32)                                                                      purpose,
       RPAD(' ',8)                                                                                                      contr_datincept,
       RPAD(' ',8)                                                                                                      contr_enddate,
       RPAD(' ',32)                                                                                                     tariff,
       RPAD(' ',4)                                                                                                          paym_period,
       RPAD(' ',2)                                                                                                          paym_mode,
       ' '                                                                                                              paym_type,
       RPAD(' ',2)                                                                                                      type_acquis,
       ' '                                                                                                              life_increaseyn,
       ' '                                                                                                              life_lentyn,
       RPAD(' ',32)                                                                                                     contr_status,
       RPAD(' ',5)                                                                                                              intermed_type,
       RPAD(' ',12)                                                                                                     fk_csmno,
       RPAD(' ',12)                                                                                                     bic_prempayor
           FROM mv_users_data u
                                                left join imt_country_code i on i.country_iso = u.network_code
                                WHERE
            u.user_type in ('CHANNEL','SUBSCRIBER')
            and u.status <> 'N' and u.wallet_status <> 'N'
                                                AND ( U.MSISDN = :MSISDN OR 'ALL' =  :MSISDN )
ORDER   BY
            custno,
            businesstype,
            accno,
            businessno,
            acc_currencyiso
)

