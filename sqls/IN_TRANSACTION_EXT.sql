/*
** SCRIPT SQL : IN_TRANSACTION_EXT-1.1.1.sql
** DATE       : 25/05/2018
** MODIFIED DATE: 14/08/2019
** DESCRIPTION: TR_SP_11 column is filled with the local currency 
				TR_NP_03 is filled with amount
				Removal of DBREF_TXN_DWH

*/
with
mv_txn_header as (
select /*+ INDEX(th) */
        case
            when trunc(modified_on) >= trunc(transfer_date) and trunc(modified_on) < to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS') then trunc(modified_on)
            else trunc(transfer_date)
        end as_of_date,
        created_on,
        created_by,
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
mv_users_data as (
select /*+ INDEX(mp) */
    mw.wallet_number,
    mc.domain_code,
    decode(mw.user_type,'OPERATOR', mw.user_type, nvl(mp.user_type, u.user_type)) as user_type,
    upper(trim(nvl(mp.designation, u.designation))) as designation,
    upper(trim(nvl(mp.state, u.state))) as state,
    nvl(mp.network_code, u.network_code) network_code
from mtx_wallet mw
    left join mtx_party mp on (mw.user_id = mp.user_id and mw.user_type = mp.user_type)
    left join users u on (mw.user_id = u.user_id and mw.user_type <> 'SUBSCRIBER')
    left join mtx_categories mc on (mc.category_code = nvl(mp.category_code, u.category_code))
where mw.user_type = 'OPERATOR' or mp.user_id is not null or u.user_id is not null
),
 txn as(
 select /*+ INDEX(TIS) INDEX(TIR)*/
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
        where PREFERENCE_CODE = 'CURRENCY_FACTOR'),100)as transaction_amount,
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
  tis.transfer_id,
  tis.previous_balance/nvl((select DEFAULT_VALUE
        from  MTX_SYSTEM_PREFERENCES
        where PREFERENCE_CODE = 'CURRENCY_FACTOR'),100)as sender_pre_balance,
        tis.post_balance/nvl((select DEFAULT_VALUE
        from  MTX_SYSTEM_PREFERENCES
        where PREFERENCE_CODE = 'CURRENCY_FACTOR'),100)as sender_post_balance,
        tis.transfer_subtype,
        tir.previous_balance/nvl((select DEFAULT_VALUE
        from  MTX_SYSTEM_PREFERENCES
        where PREFERENCE_CODE = 'CURRENCY_FACTOR'),100)as receiver_pre_balance,
        tir.post_balance/nvl((select DEFAULT_VALUE
        from  MTX_SYSTEM_PREFERENCES
        where PREFERENCE_CODE = 'CURRENCY_FACTOR'),100)as receiver_post_balance
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
			 ),
txn_full as(
select
nvl2(txn.sender_pseudo_user_id, 'PSEUDO', dus.user_type) sender_user_type,
txn.sender_wallet_number,
th.created_on as CREATED_ON_NUM,
th.transfer_date AS TRANSFER_DATE_NUM,
th.created_by,
th.transfer_id,
    CASE WHEN (dur.user_type = 'SUBSCRIBER' AND txn.TXN_MODE is not null and RTRIM(SUBSTR (txn.TXN_MODE,1,instr(txn.TXN_MODE,'#',1,1)-1),'0123456789') is null AND ABS(SUBSTR (txn.TXN_MODE,1,instr(txn.TXN_MODE,'#',1,1)-1)) = 1 )
        THEN (  CASE WHEN (SUBSTR (txn.TXN_MODE,instr(txn.TXN_MODE,'#',1,3)+1,instr(txn.TXN_MODE,'#',1,4)-instr(txn.TXN_MODE,'#',1,3)-1) = '1IRTV3')
                                        AND (UPPER(SUBSTR (txn.TXN_MODE,instr(txn.TXN_MODE,'#',1,11)+1,instr(txn.TXN_MODE,'#',1,12)-instr(txn.TXN_MODE,'#',1,11)-1)) = 'WORLDREMIT')
                                        AND (SUBSTR (txn.TXN_MODE,instr(txn.TXN_MODE,'#',1,4)+1,instr(txn.TXN_MODE,'#',1,5)-instr(txn.TXN_MODE,'#',1,4)-1) = 'GB')
        THEN NULL
                    WHEN SUBSTR (txn.TXN_MODE,instr(txn.TXN_MODE,'#',1,5)+1,instr(txn.TXN_MODE,'#',1,6)-instr(txn.TXN_MODE,'#',1,5)-1) is not null
        THEN UPPER(SUBSTR (txn.TXN_MODE,instr(txn.TXN_MODE,'#',1,5)+1,instr(txn.TXN_MODE,'#',1,6)-instr(txn.TXN_MODE,'#',1,5)-1))
                    ELSE (select PARAM_VAL_TEXT as PARAM_VAL_TEXT from DBREF_VAR_ENV
                            WHERE PARAM_KEY = 'CURRENCY_CODE')
                            END)
        ELSE (  CASE WHEN dus.designation = 'IRTIN' OR dus.designation = 'IRT' OR dus.designation = 'MFS' OR dus.designation = 'TRANSINTER'
                                THEN null
                        WHEN dus.designation like 'IRTIN%'
                                THEN (select CURRENCY_ISO from IMT_COUNTRY_CODE where COUNTRY_ISO = SUBSTR(dus.designation,-2,2))
                        ELSE (select PARAM_VAL_TEXT as PARAM_VAL_TEXT from DBREF_VAR_ENV
                                WHERE PARAM_KEY = 'CURRENCY_CODE')
                        END)
        END as sender_currency,
    nvl(txn.transaction_amount,0) as transaction_amount,
          CASE WHEN (dus.user_type = 'SUBSCRIBER' AND txn.TXN_MODE is not null and RTRIM(SUBSTR (txn.TXN_MODE,1,instr(txn.TXN_MODE,'#',1,1)-1),'0123456789') is null AND ABS(SUBSTR (txn.TXN_MODE,1,instr(txn.TXN_MODE,'#',1,1)-1)) = 1 )
                THEN (  CASE WHEN(SUBSTR (txn.TXN_MODE,instr(txn.TXN_MODE,'#',1,4)+1,instr(txn.TXN_MODE,'#',1,5)-instr(txn.TXN_MODE,'#',1,4)-1) is not null)
                                        THEN UPPER(SUBSTR (txn.TXN_MODE,instr(txn.TXN_MODE,'#',1,4)+1,instr(txn.TXN_MODE,'#',1,5)-instr(txn.TXN_MODE,'#',1,4)-1))
                                ELSE nvl((select PARAM_VAL_TEXT as PARAM_VAL_TEXT from DBREF_VAR_ENV
                                        WHERE PARAM_KEY = 'COMPLIANCE_INSTITUTE'), dur.network_code)
                                END)
        ELSE (  CASE WHEN dur.designation = 'IRTOUT' OR dur.designation = 'IRT' OR dur.designation = 'MFS' OR dur.designation = 'TRANSINTER'
                                THEN null
                        WHEN dur.designation like 'IRTOUT%'
                                THEN SUBSTR(dur.designation,-2,2)
                        ELSE (nvl((select PARAM_VAL_TEXT as PARAM_VAL_TEXT from DBREF_VAR_ENV
                                        WHERE PARAM_KEY = 'COMPLIANCE_INSTITUTE'), dur.network_code))
                        END)
        END as receiver_country,
    txn.receiver_wallet_number,
    txn.receiver_user_id,
	    nvl((select max(tag_name) keep (dense_rank first order by tag_id)
        from dbref_tags dt
        where dt.transfer_subtype = th.transfer_subtype
        and (dt.service_type is null or dt.service_type = txn.service_type)
        and (dt.sender_user_id is null or dt.sender_user_id = txn.sender_user_id)
        and (dt.sender_designation is null or dt.sender_designation = dus.designation)
        and (dt.receiver_designation is null or dt.receiver_designation = dur.designation)
        and (dt.sender_domain_code is null or dt.sender_domain_code = dus.domain_code)
        and (dt.receiver_domain_code is null or dt.receiver_domain_code = dur.domain_code)
        and (dt.sender_state is null or dt.sender_state = dus.state)
        and (dt.receiver_state is null or dt.receiver_state = dur.state)
        and (dt.not_receiver_state is null or dur.state <> dt.not_receiver_state)
        and (dt.reference_number2 is null or upper(substr(trim(th.reference_number),1,2)) = dt.reference_number2)
        and (dt.source_txn_id is null or upper(substr(trim(th.attr_2_value),1,2)) = dt.source_txn_id)
        ), th.transfer_subtype) as tag_name,
    nvl2(txn.receiver_pseudo_user_id, 'PSEUDO', dur.user_type) receiver_user_type,
    txn.receiver_msisdn,
    CASE WHEN (dus.user_type = 'SUBSCRIBER' AND txn.TXN_MODE is not null and RTRIM(SUBSTR (txn.TXN_MODE,1,instr(txn.TXN_MODE,'#',1,1)-1),'0123456789') is null AND ABS(SUBSTR (txn.TXN_MODE,1,instr(txn.TXN_MODE,'#',1,1)-1)) = 1 )
                THEN (  CASE WHEN(SUBSTR (txn.TXN_MODE,instr(txn.TXN_MODE,'#',1,5)+1,instr(txn.TXN_MODE,'#',1,6)-instr(txn.TXN_MODE,'#',1,5)-1) is not null)
                                        THEN UPPER(SUBSTR (txn.TXN_MODE,instr(txn.TXN_MODE,'#',1,5)+1,instr(txn.TXN_MODE,'#',1,6)-instr(txn.TXN_MODE,'#',1,5)-1))
                                ELSE (select PARAM_VAL_TEXT as PARAM_VAL_TEXT from DBREF_VAR_ENV
                                        WHERE PARAM_KEY = 'CURRENCY_CODE')
                                END)
        ELSE (  CASE WHEN dur.designation = 'IRTOUT' OR dur.designation = 'IRT' OR dur.designation = 'MFS' OR dur.designation = 'TRANSINTER'
                                THEN null
                        WHEN dur.designation like 'IRTOUT%'
                                THEN (select CURRENCY_ISO from IMT_COUNTRY_CODE where COUNTRY_ISO = SUBSTR(dur.designation,-2,2))
                        ELSE (select PARAM_VAL_TEXT as PARAM_VAL_TEXT from DBREF_VAR_ENV
                                        WHERE PARAM_KEY = 'CURRENCY_CODE')
                        END)
        END as receiver_currency,
    CASE
                WHEN (dur.user_type = 'SUBSCRIBER' AND txn.TXN_MODE is not null and RTRIM(SUBSTR (txn.TXN_MODE,1,instr(txn.TXN_MODE,'#',1,1)-1),'0123456789') is null AND ABS(SUBSTR (txn.TXN_MODE,1,instr(txn.TXN_MODE,'#',1,1)-1)) = 1 )
                        THEN (CASE
                                        WHEN (SUBSTR (txn.TXN_MODE,instr(txn.TXN_MODE,'#',1,3)+1,instr(txn.TXN_MODE,'#',1,4)-instr(txn.TXN_MODE,'#',1,3)-1) = '1IRTV3')
                                                AND (UPPER(SUBSTR (txn.TXN_MODE,instr(txn.TXN_MODE,'#',1,11)+1,instr(txn.TXN_MODE,'#',1,12)-instr(txn.TXN_MODE,'#',1,11)-1)) = 'WORLDREMIT')
                                                AND (SUBSTR (txn.TXN_MODE,instr(txn.TXN_MODE,'#',1,4)+1,instr(txn.TXN_MODE,'#',1,5)-instr(txn.TXN_MODE,'#',1,4)-1) = 'GB')
                                                THEN NULL
                                        WHEN SUBSTR (txn.TXN_MODE,instr(txn.TXN_MODE,'#',1,4)+1,instr(txn.TXN_MODE,'#',1,5)-instr(txn.TXN_MODE,'#',1,4)-1) is not null
                                                THEN UPPER(SUBSTR (txn.TXN_MODE,instr(txn.TXN_MODE,'#',1,4)+1,instr(txn.TXN_MODE,'#',1,5)-instr(txn.TXN_MODE,'#',1,4)-1))
                                        ELSE (nvl((select PARAM_VAL_TEXT as PARAM_VAL_TEXT from DBREF_VAR_ENV
                                                WHERE PARAM_KEY = 'COMPLIANCE_INSTITUTE'), dus.network_code))
                                END)
                ELSE ( CASE
                                        WHEN dus.designation = 'IRTIN' OR dus.designation = 'IRT' OR dus.designation = 'MFS' OR dus.designation = 'TRANSINTER'
                                                THEN null
                                        WHEN dus.designation like 'IRTIN%'
                                                THEN SUBSTR(dus.designation,-2,2)
                                        ELSE (nvl((select PARAM_VAL_TEXT as PARAM_VAL_TEXT from DBREF_VAR_ENV
                                                WHERE PARAM_KEY = 'COMPLIANCE_INSTITUTE'), dus.network_code))
                                END)
        END as sender_country,
    txn.sender_msisdn,
    txn.sender_user_id,
    decode(th.transfer_status,'TS','Y',decode(th.reconciliation_by, null, case when th.service_type = 'RC' and th.attr_2_name = 'FAILED_AT_IN' and th.attr_2_value = 'Y' then 'Y' else 'N' end,'Y')) as transfer_done,
    nvl(txn.sender_pre_balance,0) as sender_pre_balance,
    nvl(txn.sender_post_balance,0) as sender_post_balance,
    txn.transfer_subtype,
    txn.service_type,
    kyc.rec_dob as unreg_dob,
    kyc.rec_idno as unreg_id_number,
    kyc.rec_lname as unreg_last_name,
    kyc.rec_fname as unreg_first_name,
    nvl(txn.receiver_pre_balance,0) as receiver_pre_balance,
    nvl(txn.receiver_post_balance,0) as receiver_post_balance
 from
mv_txn_header th
    inner join txn
	on (th.transfer_id = txn.transfer_id and txn.transaction_type = 'MR')
	inner join sys_service_types sst on (txn.service_type = sst.service_type)
    left join mv_users_data dus on (dus.wallet_number = txn.sender_wallet_number)
    left join mv_users_data dur on (dur.wallet_number = txn.receiver_wallet_number)
    left join recieverkyc kyc on (kyc.agcout_transfer_id = th.transfer_id)
	where (sst.is_financial = 'Y' or txn.total_sc > 0)
	)
SELECT  '"'|| CUST_INSTITUTE	||'"'||'|'
||'"'||	CUST_CUSTNO	||'"'||'|'
||'"'||	ACC_BUSINESSTYPE	||'"'||'|'
||'"'||	ACC_ACCNO	||'"'||'|'
||'"'||	ACC_BUSINESSNO	||'"'||'|'
||'"'||	ACC_CURRENCYISO	||'"'||'|'
||'"'||	ENTRYDATE	||'"'||'|'
||'"'||	VALUEDATE	||'"'||'|'
||'"'||	BUSINESSNO_TRANS	||'"'||'|'
||'"'||	TR_SP_01	||'"'||'|'
||'"'||	TR_SP_02	||'"'||'|'
||'"'||	TR_SP_03	||'"'||'|'
||'"'||	TR_SP_04	||'"'||'|'
||'"'||	TR_SP_05	||'"'||'|'
||'"'||	TR_SP_06	||'"'||'|'
||'"'||	TR_SP_07	||'"'||'|'
||'"'||	TR_SP_08	||'"'||'|'
||'"'||	TR_SP_09	||'"'||'|'
||'"'||	TR_SP_10	||'"'||'|'
||'"'||	TR_SP_11	||'"'||'|'
||'"'||	TR_SP_12	||'"'||'|'
||'"'||	TR_SP_13	||'"'||'|'
||'"'||	TR_SP_14	||'"'||'|'
||'"'||	TR_SP_15	||'"'||'|'
||'"'||	TR_SP_16	||'"'||'|'
||'"'||	TR_SP_17	||'"'||'|'
||'"'||	TR_SP_18	||'"'||'|'
||'"'||	TR_SP_19	||'"'||'|'
||'"'||	TR_SP_20	||'"'||'|'
||'"'||	TR_SP_21	||'"'||'|'
||'"'||	TR_SP_22	||'"'||'|'
||'"'||	TR_SP_23	||'"'||'|'
||'"'||	TR_SP_24	||'"'||'|'
||'"'||	TR_SP_25	||'"'||'|'
||'"'||	TR_SP_26	||'"'||'|'
||'"'||	TR_SP_27	||'"'||'|'
||'"'||	TR_SP_28	||'"'||'|'
||'"'||	TR_SP_29	||'"'||'|'
||'"'||	TR_SP_30	||'"'||'|'
||'"'||	TR_SP_31	||'"'||'|'
||'"'||	TR_SP_32	||'"'||'|'
||'"'||	TR_SP_33	||'"'||'|'
||'"'||	TR_SP_34	||'"'||'|'
||'"'||	TR_SP_35	||'"'||'|'
||'"'||	TR_SP_36	||'"'||'|'
||'"'||	TR_SP_37	||'"'||'|'
||'"'||	TR_SP_38	||'"'||'|'
||'"'||	TR_SP_39	||'"'||'|'
||'"'||	TR_SP_40	||'"'||'|'
||'"'||	TR_SP_41	||'"'||'|'
||'"'||	TR_SP_42	||'"'||'|'
||'"'||	TR_SP_43	||'"'||'|'
||'"'||	TR_SP_44	||'"'||'|'
||'"'||	TR_SP_45	||'"'||'|'
||'"'||	TR_SP_46	||'"'||'|'
||'"'||	TR_SP_47	||'"'||'|'
||'"'||	TR_SP_48	||'"'||'|'
||	TR_NP_01	         ||'|'
||	TR_NP_02	         ||'|'
||	TR_NP_03	         ||'|'
||	TR_NP_04	         ||'|'
||	TR_NP_05	         ||'|'
||	TR_NP_06	         ||'|'
||	TR_NP_07	         ||'|'
||	TR_NP_08	         ||'|'
||	TR_NP_09	         ||'|'
||	TR_NP_10
from (
select * from
(SELECT
		:INSTITUTE	                                    						cust_institute,
		SUBSTR(t.sender_user_type,1,1) || nvl(t.sender_msisdn,'XXXXXXXX')		   	cust_custno,
		nvl(substr(t.sender_wallet_number,1,2),'XXXX')		             			acc_businesstype,
		nvl(substr(t.sender_wallet_number,3,9),'XXXX')		                     	acc_accno,
		nvl(substr(t.sender_wallet_number,12,9),'XXXX')  		                   	acc_businessno,
	case
    when (select max(PARAM_VAL_TEXT) LOCAL_CURRENCY   from DBREF_VAR_ENV
				where PARAM_KEY = 'CURRENCY_CODE')='GNF' then 'GBP'
    else
    (select max(PARAM_VAL_TEXT) LOCAL_CURRENCY   from DBREF_VAR_ENV
				where PARAM_KEY = 'CURRENCY_CODE')	end as								acc_currencyiso,
		TO_CHAR(t.created_on_num,'YYYYMMDD')         									entrydate,
		TO_CHAR(t.transfer_date_num,'YYYYMMDD')      									valuedate,
		SUBSTR(t.transfer_id,1,2)||SUBSTR(t.transfer_id,instr(t.transfer_id,'.')+1)		businessno_trans,
		t.tag_name														           	tr_sp_01,
		substr(t.unreg_first_name,1,64)								            	tr_sp_02,
		substr(t.unreg_first_name,65)		     									tr_sp_03,
		substr(t.unreg_last_name,1,64)												tr_sp_04,
		substr(t.unreg_last_name,65)								                tr_sp_05,
		t.unreg_id_number															tr_sp_06,
		to_char(t.unreg_dob,'YYYYMMDD')						tr_sp_07,
		t.created_by																tr_sp_08,
		t.service_type	        													tr_sp_09,
		t.transfer_subtype         													tr_sp_10,
		(select max(PARAM_VAL_TEXT) LOCAL_CURRENCY   from DBREF_VAR_ENV
				where PARAM_KEY = 'CURRENCY_CODE')									tr_sp_11,
		t.transfer_id																tr_sp_12,
		null																		tr_sp_13,
		null																		tr_sp_14,
		null																		tr_sp_15,
		null																		tr_sp_16,
		null																		tr_sp_17,
		null																		tr_sp_18,
		null																		tr_sp_19,
		null																		tr_sp_20,
		null																		tr_sp_21,
		null																		tr_sp_22,
		null																		tr_sp_23,
		null																		tr_sp_24,
		null																		tr_sp_25,
		null																		tr_sp_26,
		null																		tr_sp_27,
		null																		tr_sp_28,
		null																		tr_sp_29,
		null																		tr_sp_30,
		null																		tr_sp_31,
		null																		tr_sp_32,
		null																		tr_sp_33,
		null																		tr_sp_34,
		null																		tr_sp_35,
		null																		tr_sp_36,
		null																		tr_sp_37,
		null																		tr_sp_38,
		null																		tr_sp_39,
		null																		tr_sp_40,
		null																		tr_sp_41,
		null																		tr_sp_42,
		null																		tr_sp_43,
		null																		tr_sp_44,
		null																		tr_sp_45,
		null																		tr_sp_46,
		null																		tr_sp_47,
		null																		tr_sp_48,
		TO_CHAR(t.sender_pre_balance, 'S0000000000000.00') 							tr_np_01,
		TO_CHAR(t.sender_post_balance, 'S0000000000000.00')							tr_np_02,
		TO_CHAR(-t.transaction_amount, 'S0000000000000.00')							tr_np_03,
		to_number(null)																tr_np_04,
		to_number(null)																tr_np_05,
		to_number(null)																tr_np_06,
		to_number(null)																tr_np_07,
		to_number(null)																tr_np_08,
		to_number(null)																tr_np_09,
		to_number(null)																tr_np_10
FROM    txn_full t
		left join imt_country_code scur on scur.COUNTRY_ISO = t.sender_country
WHERE   t.sender_user_type in ('SUBSCRIBER','CHANNEL')
        AND ( t.sender_msisdn = :MSISDN OR 'ALL' =  :MSISDN )
        AND t.transfer_done = 'Y'
		 AND  (t.transfer_date_num   >=  to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS') -1  AND t.transfer_date_num   <  to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS'))
UNION ALL
SELECT
		:INSTITUTE	                                    						cust_institute,
		SUBSTR(t.receiver_user_type,1,1) || nvl(t.receiver_msisdn,'XXXXXXXX')		   	cust_custno,
		nvl(substr(t.receiver_wallet_number,1,2),'XXXX')		             			acc_businesstype,
		nvl(substr(t.receiver_wallet_number,3,9),'XXXX')		                     	acc_accno,
		nvl(substr(t.receiver_wallet_number,12,9),'XXXX')  		                   	acc_businessno,
	case
    when (select max(PARAM_VAL_TEXT) LOCAL_CURRENCY   from DBREF_VAR_ENV
				where PARAM_KEY = 'CURRENCY_CODE')='GNF' then 'GBP'
    else
    (select max(PARAM_VAL_TEXT) LOCAL_CURRENCY   from DBREF_VAR_ENV
				where PARAM_KEY = 'CURRENCY_CODE')	end as								acc_currencyiso,
		TO_CHAR(t.created_on_num,'YYYYMMDD')         									entrydate,
		TO_CHAR(t.transfer_date_num,'YYYYMMDD')      									valuedate,
		SUBSTR(t.transfer_id,1,2)||SUBSTR(t.transfer_id,instr(t.transfer_id,'.')+1)		businessno_trans,
		t.tag_name														           	tr_sp_01,
		substr(t.unreg_first_name,1,64)								            	tr_sp_02,
		substr(t.unreg_first_name,65)		     									tr_sp_03,
		substr(t.unreg_last_name,1,64)												tr_sp_04,
		substr(t.unreg_last_name,65)								                tr_sp_05,
		t.unreg_id_number															tr_sp_06,
		to_char(t.unreg_dob,'YYYYMMDD')						tr_sp_07,
		t.created_by																tr_sp_08,
		t.service_type	        													tr_sp_09,
		t.transfer_subtype         													tr_sp_10,
		(select max(PARAM_VAL_TEXT) LOCAL_CURRENCY   from DBREF_VAR_ENV
				where PARAM_KEY = 'CURRENCY_CODE')									tr_sp_11,
		t.transfer_id																tr_sp_12,
		null																		tr_sp_13,
		null																		tr_sp_14,
		null																		tr_sp_15,
		null																		tr_sp_16,
		null																		tr_sp_17,
		null																		tr_sp_18,
		null																		tr_sp_19,
		null																		tr_sp_20,
		null																		tr_sp_21,
		null																		tr_sp_22,
		null																		tr_sp_23,
		null																		tr_sp_24,
		null																		tr_sp_25,
		null																		tr_sp_26,
		null																		tr_sp_27,
		null																		tr_sp_28,
		null																		tr_sp_29,
		null																		tr_sp_30,
		null																		tr_sp_31,
		null																		tr_sp_32,
		null																		tr_sp_33,
		null																		tr_sp_34,
		null																		tr_sp_35,
		null																		tr_sp_36,
		null																		tr_sp_37,
		null																		tr_sp_38,
		null																		tr_sp_39,
		null																		tr_sp_40,
		null																		tr_sp_41,
		null																		tr_sp_42,
		null																		tr_sp_43,
		null																		tr_sp_44,
		null																		tr_sp_45,
		null																		tr_sp_46,
		null																		tr_sp_47,
		null																		tr_sp_48,
		TO_CHAR(t.receiver_pre_balance, 'S0000000000000.00') 						tr_np_01,
		TO_CHAR(t.receiver_post_balance, 'S0000000000000.00')						tr_np_02,
		TO_CHAR(t.transaction_amount, 'S0000000000000.00')							tr_np_03,
		to_number(null)																tr_np_04,
		to_number(null)																tr_np_05,
		to_number(null)																tr_np_06,
		to_number(null)																tr_np_07,
		to_number(null)																tr_np_08,
		to_number(null)																tr_np_09,
		to_number(null)																tr_np_10
FROM    txn_full t
		left join imt_country_code rcur on rcur.country_iso = t.receiver_country
WHERE   t.receiver_user_type in ('SUBSCRIBER','CHANNEL')
        AND ( t.receiver_msisdn = :MSISDN OR 'ALL' =  :MSISDN )
        AND t.transfer_done = 'Y'
		 AND  (t.transfer_date_num   >=  to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS') -1  AND t.transfer_date_num   <  to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS')) 
)		 
ORDER   BY  cust_custno,     
            acc_businesstype,
            acc_accno,       
            acc_businessno,  
            acc_currencyiso, 
            entrydate,       
            valuedate,       
            businessno_trans
)
