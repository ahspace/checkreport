  /*
** SCRIPT SQL : IN_TRANSACTION-1.2.1.sql
** DATE       : 25/05/2018
** MODIFIED DATE: 10/08/2019
** AUTHOR: ZMSH2370
** DESCRIPTION: Logical change in FK_CURRENCY and AMOUNTORIG
Removal of DBREF_TXN_DWH
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
where transfer_date < to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS')-1
    and modified_on >= to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS')-1
    and modified_on < to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS')
)
, mv_txn_header as (
select /*+ INDEX(th) */
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
	select /*+ INDEX(tira) INDEX(tisa)*/
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
th.transfer_date AS TRANSFER_DATE_NUM,
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
    decode(th.transfer_status,'TS','Y',decode(th.reconciliation_by, null, case when th.service_type = 'RC' and th.attr_2_name = 'FAILED_AT_IN' and th.attr_2_value = 'Y' then 'Y' else 'N' end,'Y')) as transfer_done

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
	)

SELECT	CUST_INSTITUTE
||	CUST_CUSTNO
||	ACC_BUSINESSTYPE
||	ACC_ACCNO
||	ACC_BUSINESSNO
||	ACC_CURRENCYISO
||	ENTRYDATE
||	VALUEDATE
||	BUSINESSNO_TRANS
||	TXTKEY
||	PRN
||	FK_CURRENCY
||	AMOUNT
||	AMOUNTORIG
||	BRANCH_OFFICE
||	CONTRA_COUNTRY
||	CONTRA_ACCNO
||	CONTRA_ZIP
||	CONTRA_NAME
||	CSHYN
||	REASON1
||	REASON2
||	REASON3
||	REASON4
||	STATUS
||	CUST_FLAGS
||	TR_EMPLNO
||	TRANSTIMESTAMP
||	CONTRA_CUSTNO
||	ANALYTICAL_TRANS_CODE
||	EARLY_LIQUIDATION_FLAG
||	CONTRA_EMPLNO
||	CONTRA_ACC_OWNER_FLAG
||	OWNER_EMPLNO
||	CONTRA_BUSINESSTYPE
||	CONTRA_H_ACCNO
||	CONTRA_BUSINESSNO
||	CONTRA_ACC_CURRENCYISO
||	FILLER
FROM (
SELECT * FROM
(SELECT
       RPAD(:INSTITUTE,4)                                    						cust_institute,
       RPAD(SUBSTR(t.sender_user_type,1,1) || nvl(t.sender_msisdn,'XXXXXXXX'),16)   cust_custno,
       RPAD(nvl(substr(t.sender_wallet_number,1,2),'XXXX'),4)             			acc_businesstype,
       RPAD(nvl(substr(t.sender_wallet_number,3,9),'XXXX'),11)                     	acc_accno,
       RPAD(nvl(substr(t.sender_wallet_number,12,9),'XXXX'),11)                     acc_businessno,
	case
    when (select max(PARAM_VAL_TEXT) LOCAL_CURRENCY   from DBREF_VAR_ENV
				where PARAM_KEY = 'CURRENCY_CODE')='GNF' then 'GBP'
    else
    (select max(PARAM_VAL_TEXT) LOCAL_CURRENCY   from DBREF_VAR_ENV
				where PARAM_KEY = 'CURRENCY_CODE')	end as							acc_currencyiso,
       TO_CHAR(t.created_on_num,'YYYYMMDD')          								entrydate,
       TO_CHAR(t.transfer_date_num,'YYYYMMDD')       								valuedate,
       RPAD(SUBSTR(t.transfer_id,1,2)||SUBSTR(t.transfer_id,instr(t.transfer_id,'.')+1),16)  businessno_trans,
       RPAD(' ',2)                													txtkey,
       RPAD(' ',8)                              				        			prn,
	   	CASE
		WHEN    scur.CURRENCY_ISO is not null
			then scur.CURRENCY_ISO
		when t.sender_currency is not null and t.sender_currency <> 'NA'
			then t.sender_currency
		else (select max(PARAM_VAL_TEXT) LOCAL_CURRENCY   from DBREF_VAR_ENV
				where PARAM_KEY = 'CURRENCY_CODE')
		END 																		fk_currency,
       TO_CHAR(nvl(-t.transaction_amount,0), 'S0000000000000.00')         			amount,
       TO_CHAR(0, 'S0000000000000.00')               								amountorig,
	   RPAD(' ',10)                          										branch_office,
       RPAD(nvl(t.receiver_country,' '),3)		                         			contra_country,
	   RPAD(t.receiver_wallet_number,35)		 			                        contra_accno,
       RPAD(' ',12)                          										contra_zip,
       RPAD(t.receiver_user_id,27)                          						contra_name,
       CASE
	   when t.tag_name like 'CASHIN%' or t.tag_name like '%CASHOUT%'
	   then 'Y'
	   else 'N'  end									                            cshyn,
       RPAD(' ',27)      										                    reason1,
       RPAD(' ',27)    											                    reason2,
       RPAD(' ',27)        		         									        reason3,
       RPAD(' ',27)          											            reason4,
	   ' '                                       									status,
       RPAD(' ',24) 							                                    cust_flags,
       RPAD(' ',16)  										                        tr_emplno,
       RPAD(TO_CHAR(t.transfer_date_num,'HH24:MI:SS'),17)                          	transtimestamp,
       RPAD(SUBSTR(t.receiver_user_type,1,1) ||nvl(t.receiver_msisdn,'XXXXXXXX'),16)              	contra_custno,
       RPAD(SUBSTR(t.transfer_id,1,2),6)                           				analytical_trans_code,
       ' '                                       									early_liquidation_flag,
       RPAD(' ',16)		                         									contra_emplno,
       ' '                                       									contra_acc_owner_flag,
       RPAD(' ',16)		                         									owner_emplno,
       RPAD(' ',4)     		                     									contra_businesstype,
       RPAD(' ',11)                  		     									contra_h_accno,
       RPAD(' ',11)                         	 									contra_businessno,
       RPAD(decode(t.receiver_currency,'NA',nvl(rcur.CURRENCY_ISO,'XX'),null,'XX',t.receiver_currency),3)      contra_acc_currencyiso,
       RPAD(' ',7)                           	 									filler
FROM    txn_full t
		left join imt_country_code scur on scur.COUNTRY_ISO = t.sender_country
		left join imt_country_code rcur on rcur.country_iso = t.receiver_country
WHERE   t.sender_user_type in ('SUBSCRIBER','CHANNEL')
        AND ( t.sender_msisdn = :MSISDN OR 'ALL' =  :MSISDN )
        AND t.transfer_done = 'Y'
		 AND  (t.transfer_date_num   >=  to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS') -1  AND t.transfer_date_num   <  to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS'))
UNION ALL
SELECT
       RPAD(:INSTITUTE,4)                                    						cust_institute,
       RPAD(SUBSTR(t.receiver_user_type,1,1) || nvl(t.receiver_msisdn,'XXXXXXXX'),16)   cust_custno,
       RPAD(nvl(substr(t.receiver_wallet_number,1,2),'XXXX'),4)             			acc_businesstype,
       RPAD(nvl(substr(t.receiver_wallet_number,3,9),'XXXX'),11)                     	acc_accno,
       RPAD(nvl(substr(t.receiver_wallet_number,12,9),'XXXX'),11)                     acc_businessno,
	case
    when (select max(PARAM_VAL_TEXT) LOCAL_CURRENCY   from DBREF_VAR_ENV
				where PARAM_KEY = 'CURRENCY_CODE')='GNF' then 'GBP'
    else
    (select max(PARAM_VAL_TEXT) LOCAL_CURRENCY   from DBREF_VAR_ENV
				where PARAM_KEY = 'CURRENCY_CODE')	end as								acc_currencyiso,
       TO_CHAR(t.created_on_num,'YYYYMMDD')          									entrydate,
       TO_CHAR(t.transfer_date_num,'YYYYMMDD')       									valuedate,
       RPAD(SUBSTR(t.transfer_id,1,2)||SUBSTR(t.transfer_id,instr(t.transfer_id,'.')+1),16)  businessno_trans,
       RPAD(' ',2)                													txtkey,
       RPAD(' ',8)                              				        			prn,
	  	CASE
		WHEN    scur.CURRENCY_ISO is not null
			then scur.CURRENCY_ISO
		when t.sender_currency is not null and t.sender_currency <> 'NA'
			then t.sender_currency
		else (select max(PARAM_VAL_TEXT) LOCAL_CURRENCY   from DBREF_VAR_ENV
				where PARAM_KEY = 'CURRENCY_CODE')
		END  																		    fk_currency,
       TO_CHAR(nvl(t.transaction_amount,0), 'S0000000000000.00')         			amount,
       TO_CHAR(0, 'S0000000000000.00')               								amountorig,
	   RPAD(' ',10)                          										branch_office,
       RPAD(nvl(t.sender_country,' '),3)		                            	contra_country,
	   RPAD(t.sender_wallet_number,35)		 			                        contra_accno,
       RPAD(' ',12)                          										contra_zip,
       RPAD(t.sender_user_id,27)                          						contra_name,
       CASE
	   when t.tag_name like 'CASHIN%' or t.tag_name like '%CASHOUT%'
	   then 'Y'
	   else 'N'  end									                            cshyn,
       RPAD(' ',27)      										                    reason1,
       RPAD(' ',27)    											                    reason2,
       RPAD(' ',27)        		         									        reason3,
       RPAD(' ',27)          											            reason4,
	   ' '                                       									status,
       RPAD(' ',24) 							                                    cust_flags,
       RPAD(' ',16)  										                        tr_emplno,
       RPAD(TO_CHAR(t.transfer_date_num,'HH24:MI:SS'),17)                          	transtimestamp,
       RPAD(SUBSTR(t.sender_user_type,1,1) ||nvl(t.sender_msisdn,'XXXXXXXX'),16)              	contra_custno,
       RPAD(SUBSTR(t.transfer_id,1,2),6)                           				analytical_trans_code,
       ' '                                       									early_liquidation_flag,
       RPAD(' ',16)		                         									contra_emplno,
       ' '                                       									contra_acc_owner_flag,
       RPAD(' ',16)		                         									owner_emplno,
       RPAD(' ',4)     		                     									contra_businesstype,
       RPAD(' ',11)                  		     									contra_h_accno,
       RPAD(' ',11)                         	 									contra_businessno,
       RPAD(decode(t.sender_currency,'NA',nvl(scur.CURRENCY_ISO,'XX'),null,'XX', t.sender_currency),3)   contra_acc_currencyiso,
       RPAD(' ',7)                           	 									filler
FROM    txn_full t
		left join imt_country_code scur on scur.COUNTRY_ISO = t.sender_country
		left join imt_country_code rcur on rcur.country_iso = t.receiver_country
WHERE   t.receiver_user_type in ('SUBSCRIBER','CHANNEL')
        AND ( t.receiver_msisdn = :MSISDN OR 'ALL' =  :MSISDN )
       AND t.transfer_done = 'Y'
		 AND  (t.transfer_date_num   >=  to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS') -1  AND t.transfer_date_num   <  to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS'))
)
ORDER   BY
            cust_custno,
            acc_businesstype,
            acc_accno,
            acc_businessno,
            acc_currencyiso,
            entrydate,
            valuedate,
            businessno_trans
)