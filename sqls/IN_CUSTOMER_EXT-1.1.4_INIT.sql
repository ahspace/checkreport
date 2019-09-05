SELECT 
'"'||	INSTITUTE	||	'"|'||
'"'||	CUSTNO		||	'"|'||
'"'||	CUST_SPH_01	||	'"|'||
'"'||	CUST_SPH_02	||	'"|'||
'"'||	CUST_SPH_03	||	'"|'||
'"'||	CUST_SPH_04	||	'"|'||
'"'||	CUST_SPH_05	||	'"|'||
'"'||	CUST_SPH_06	||	'"|'||
'"'||	CUST_SPH_07	||	'"|'||
'"'||	CUST_SPH_08	||	'"|'||
'"'||	CUST_SPH_09	||	'"|'||
'"'||	CUST_SPH_10	||	'"|'||
'"'||	CUST_SPH_11	||	'"|'||
'"'||	CUST_SPH_12	||	'"|'||
'"'||	CUST_SPH_13	||	'"|'||
'"'||	CUST_SPH_14	||	'"|'||
'"'||	CUST_SPH_15	||	'"|'||
'"'||	CUST_SPH_16	||	'"|'||
'"'||	CUST_SPH_17	||	'"|'||
'"'||	CUST_SPH_18	||	'"|'||
'"'||	CUST_SPH_19	||	'"|'||
'"'||	CUST_SPH_20	||	'"|'||
'"'||	CUST_SPH_21	||	'"|'||
'"'||	CUST_SPH_22	||	'"|'||
'"'||	CUST_SPH_23	||	'"|'||
'"'||	CUST_SPH_24	||	'"|'||
'"'||	CUST_SPH_25	||	'"|'||
'"'||	CUST_SPH_26	||	'"|'||
'"'||	CUST_SPH_27	||	'"|'||
'"'||	CUST_SPH_28	||	'"|'||
'"'||	CUST_SPH_29	||	'"|'||
'"'||	CUST_SPH_30	||	'"|'||
'"'||	CUST_SPH_31	||	'"|'||
'"'||	CUST_SPH_32	||	'"|'||
'"'||	CUST_SPH_33	||	'"|'||
'"'||	CUST_SPH_34	||	'"|'||
'"'||	CUST_SPH_35	||	'"|'||
'"'||	CUST_SPH_36	||	'"|'||
'"'||	CUST_SPH_37	||	'"|'||
'"'||	CUST_SPH_38	||	'"|'||
'"'||	CUST_SPH_39	||	'"|'||
'"'||	CUST_SPH_40	||	'"|'||
'"'||	CUST_SPH_41	||	'"|'||
'"'||	CUST_SPH_42	||	'"|'||
'"'||	CUST_SPH_43	||	'"|'||
'"'||	CUST_SPH_44	||	'"|'||
'"'||	CUST_SPH_45	||	'"|'||
'"'||	CUST_SPH_46	||	'"|'||
'"'||	CUST_SPH_47	||	'"|'||
'"'||	CUST_SPH_48	||	'"|'||
	CUST_NPH_01	||	 '|'||
	CUST_NPH_02	||	 '|'||
	CUST_NPH_03	||	 '|'||
	CUST_NPH_04	||	 '|'||
	CUST_NPH_05	||	 '|'||
	CUST_NPH_06	||	 '|'||
	CUST_NPH_07	||	 '|'||
	CUST_NPH_08	||	 '|'||
	CUST_NPH_09	||	 '|'||
	CUST_NPH_10		
FROM (
SELECT
        RPAD(:INSTITUTE,4)                            	institute,              
        substr(u.user_type,1,1)||nvl(u.msisdn,' ')    		custno,             -- 
        translate(SUBSTR(u.user_name,33),'"',' ')            			cust_sph_01,        -- 
        translate(SUBSTR(u.last_name,33),'"',' ')            			cust_sph_02,        -- 
        translate(SUBSTR(u.address1,33),'"',' ')   			cust_sph_03,        -- 
        translate(u.address2,'"',' ')          			cust_sph_04,        -- 
        translate(SUBSTR(u.city,29),'"',' ')      			cust_sph_05,        -- 
        translate(u.external_code,'"',' ')        		cust_sph_06,        --
        u.id_no                            			cust_sph_07,        -- 
        u.addon_idtype						cust_sph_08,        --
        u.category_code                    			cust_sph_09,        -- 
	u.category_name						cust_sph_10, 
	decode(u.user_type,'CHANNEL',u.state,null)		cust_sph_11,
	u.created_by						cust_sph_12,
	substr(created.user_type,1,1)||created.msisdn		cust_sph_13,	
	created.category_code					cust_sph_14,
	created.category_name					cust_sph_15,
	 case 
	   WHEN decode(u.user_type,'SUBSCRIBER',u.batch_id,'CHANNEL',u.bulk_id,null) IS NOT NULL THEN 'WEB BULK' 
	   when u.created_by = u.user_id then 'USSD SELF REGISTRATION'
	   when u.ret_msisdn is not null then 'NOMAD/USSD' 
	   when u.created_by = 'SYSTEM' then 'USSD PROVISIONING'
	   ELSE 'WEB TANGO' end 				cust_sph_16,
	translate(SUBSTR(u.remarks,1,64),'"',' ')					cust_sph_17,
	translate(SUBSTR(u.remarks,65),'"',' ')					cust_sph_18,
	u.user_id						cust_sph_19,
	u.hierarchical_level					cust_sph_20,
	u.head_category_code					cust_sph_21,
	u.head_category_name					cust_sph_22,
	null									cust_sph_23,		
	null									cust_sph_24,		
	null									cust_sph_25,		
	null						cust_sph_26,
	null								cust_sph_27,
	null								cust_sph_28,
	null								cust_sph_29,
	null								cust_sph_30,
	null								cust_sph_31,
	null								cust_sph_32,
	null								cust_sph_33,
	null								cust_sph_34,
	null								cust_sph_35,
	null								cust_sph_36,
	null								cust_sph_37,
	null								cust_sph_38,
	null								cust_sph_39,
	null								cust_sph_40,
	null								cust_sph_41,
	null								cust_sph_42,
	null								cust_sph_43,
	null								cust_sph_44,
	null								cust_sph_45,
	null								cust_sph_46,
	null								cust_sph_47,
	null								cust_sph_48,
		to_number(null)							cust_nph_01, 	
		to_number(null)							cust_nph_02,	
		to_number(null)						cust_nph_03,
		to_number(null)						cust_nph_04,
		to_number(null)						cust_nph_05,
		to_number(null)						cust_nph_06,
		to_number(null)						cust_nph_07,
		to_number(null)						cust_nph_08,
		to_number(null)						cust_nph_09,
		to_number(null)						cust_nph_10
FROM    dbref_users_data u 
LEFT JOIN DBREF_USERS_DATA created on u.created_by = created.user_id
left join (select nvl(debit_tr.user_id, credit_tr.user_id) as user_id, nvl(debit_tr.user_type, credit_tr.user_type) as user_type,
				total_debit, total_credit, max_debit_date, max_credit_date, 
				case when nvl(max_credit_date,to_date('01/01/1900','DD/MM/YYYY')) > nvl(max_debit_date,to_date('01/01/1900','DD/MM/YYYY')) 
				then max_credit_date else max_debit_date end as last_transfer_date,
				case when total_debit >= (select DECODE(NETWORK_CODE,'GC',2500000,200000) P_VAL_NUM FROM NETWORKS) then 'F'
				     when total_credit >= (select DECODE(NETWORK_CODE,'GC',2500000,200000) P_VAL_NUM FROM NETWORKS) then 'F'
				else 'L' end as activity_volumetry_indicator
				from (
				(select sender_user_id as user_id, sender_user_type as user_type, sum(transaction_amount) as total_debit, max(TRANSFER_DATE_NUM) as max_debit_date
				from dbref_txn_dwh 
				where transfer_done = 'Y'
				and transfer_date_num >= to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS')-30
				group by sender_user_id, sender_user_type)  debit_tr
				full outer join 
				(select receiver_user_id as user_id, receiver_user_type as user_type, sum(transaction_amount) as total_credit, max(TRANSFER_DATE_NUM) as max_credit_date
				from dbref_txn_dwh 
				where transfer_done = 'Y'
				and transfer_date_num >= to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS')-30
				group by receiver_user_id, receiver_user_type)   credit_tr
				on debit_tr.user_id = credit_tr.user_id and debit_tr.user_type = credit_tr.user_type)) activity_data
		on u.user_id = activity_data.user_id and u.user_type = activity_data.user_type
WHERE   u.status <> 'N'
	AND u.user_type in ('SUBSCRIBER','CHANNEL')
	AND u.wallet_primary = 'Y'
        AND u.wallet_status <> 'N' 
	AND	(   (u.creation_date  >=  to_date(:TS_STARTD,'DD/MM/YYYY HH24:MI:SS') AND u.creation_date   <  to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS'))	
	         OR (u.modification_date  >=  to_date(:TS_STARTD,'DD/MM/YYYY HH24:MI:SS') AND u.modification_date  <  to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS')) )	
		and (:TYPOFFILE <> 'FULL' or (u.status <> 'N' and u.wallet_status <> 'N' and u.wallet_primary ='Y') )
        AND ( U.MSISDN = :MSISDN OR 'ALL' =  :MSISDN )
ORDER   BY  custno 
)
UNION ALL
SELECT 
'"'||	INSTITUTE	||	'"|'||
'"'||	CUSTNO		||	'"|'||
'"'||	CUST_SPH_01	||	'"|'||
'"'||	CUST_SPH_02	||	'"|'||
'"'||	CUST_SPH_03	||	'"|'||
'"'||	CUST_SPH_04	||	'"|'||
'"'||	CUST_SPH_05	||	'"|'||
'"'||	CUST_SPH_06	||	'"|'||
'"'||	CUST_SPH_07	||	'"|'||
'"'||	CUST_SPH_08	||	'"|'||
'"'||	CUST_SPH_09	||	'"|'||
'"'||	CUST_SPH_10	||	'"|'||
'"'||	CUST_SPH_11	||	'"|'||
'"'||	CUST_SPH_12	||	'"|'||
'"'||	CUST_SPH_13	||	'"|'||
'"'||	CUST_SPH_14	||	'"|'||
'"'||	CUST_SPH_15	||	'"|'||
'"'||	CUST_SPH_16	||	'"|'||
'"'||	CUST_SPH_17	||	'"|'||
'"'||	CUST_SPH_18	||	'"|'||
'"'||	CUST_SPH_19	||	'"|'||
'"'||	CUST_SPH_20	||	'"|'||
'"'||	CUST_SPH_21	||	'"|'||
'"'||	CUST_SPH_22	||	'"|'||
'"'||	CUST_SPH_23	||	'"|'||
'"'||	CUST_SPH_24	||	'"|'||
'"'||	CUST_SPH_25	||	'"|'||
'"'||	CUST_SPH_26	||	'"|'||
'"'||	CUST_SPH_27	||	'"|'||
'"'||	CUST_SPH_28	||	'"|'||
'"'||	CUST_SPH_29	||	'"|'||
'"'||	CUST_SPH_30	||	'"|'||
'"'||	CUST_SPH_31	||	'"|'||
'"'||	CUST_SPH_32	||	'"|'||
'"'||	CUST_SPH_33	||	'"|'||
'"'||	CUST_SPH_34	||	'"|'||
'"'||	CUST_SPH_35	||	'"|'||
'"'||	CUST_SPH_36	||	'"|'||
'"'||	CUST_SPH_37	||	'"|'||
'"'||	CUST_SPH_38	||	'"|'||
'"'||	CUST_SPH_39	||	'"|'||
'"'||	CUST_SPH_40	||	'"|'||
'"'||	CUST_SPH_41	||	'"|'||
'"'||	CUST_SPH_42	||	'"|'||
'"'||	CUST_SPH_43	||	'"|'||
'"'||	CUST_SPH_44	||	'"|'||
'"'||	CUST_SPH_45	||	'"|'||
'"'||	CUST_SPH_46	||	'"|'||
'"'||	CUST_SPH_47	||	'"|'||
'"'||	CUST_SPH_48	||	'"|'||
	CUST_NPH_01	||	 '|'||
	CUST_NPH_02	||	 '|'||
	CUST_NPH_03	||	 '|'||
	CUST_NPH_04	||	 '|'||
	CUST_NPH_05	||	 '|'||
	CUST_NPH_06	||	 '|'||
	CUST_NPH_07	||	 '|'||
	CUST_NPH_08	||	 '|'||
	CUST_NPH_09	||	 '|'||
	CUST_NPH_10		
FROM (
SELECT
        RPAD(:INSTITUTE,4)                       	institute,              
        'U'||nvl(UU.msisdn,' ') 	custno, 
        translate(SUBSTR(RK.rec_fname,33),'"',' ')	cust_sph_01,        -- 
        translate(SUBSTR(RK.rec_lname,33),'"',' ')	cust_sph_02,        -- 
        null   										cust_sph_03,        -- 
        null   										cust_sph_04,        -- 
        null   										cust_sph_05,        -- 
        null       									cust_sph_06,        --
        RK.rec_idno    		 						cust_sph_07,        -- 
        null										cust_sph_08,        --
        null            							cust_sph_09,        -- 
		null										cust_sph_10, 
		null										cust_sph_11,
		null										cust_sph_12,
		null										cust_sph_13,	
		null										cust_sph_14,
		null										cust_sph_15,
		null										cust_sph_16,
		null										cust_sph_17,
		null										cust_sph_18,
		uu.unreg_user_id							cust_sph_19,
		null										cust_sph_20,
		null										cust_sph_21,
		null										cust_sph_22,
		null										cust_sph_23,		
		null										cust_sph_24,		
		null										cust_sph_25,		
		null										cust_sph_26,
		null										cust_sph_27,
		null										cust_sph_28,
		null										cust_sph_29,
		null										cust_sph_30,
		null										cust_sph_31,
		null										cust_sph_32,
		null										cust_sph_33,
		null										cust_sph_34,
		null										cust_sph_35,
		null										cust_sph_36,
		null										cust_sph_37,
		null										cust_sph_38,
		null										cust_sph_39,
		null										cust_sph_40,
		null										cust_sph_41,
		null										cust_sph_42,
		null										cust_sph_43,
		null										cust_sph_44,
		null										cust_sph_45,
		null										cust_sph_46,
		null										cust_sph_47,
		null										cust_sph_48,
		to_number(null)								cust_nph_01, 	
		to_number(null)								cust_nph_02,	
		to_number(null)								cust_nph_03,
		to_number(null)								cust_nph_04,
		to_number(null)								cust_nph_05,
		to_number(null)								cust_nph_06,
		to_number(null)								cust_nph_07,
		to_number(null)								cust_nph_08,
		to_number(null)								cust_nph_09,
		to_number(null)								cust_nph_10
FROM RecieverKYC RK 
INNER JOIN MTX_Transaction_Header TH ON rk.p2p_transfer_id = TH.transfer_id
  AND (  (TH.modified_on   >= to_date(:TS_STARTD,'DD/MM/YYYY HH24:MI:SS') AND 
          TH.modified_on   <  to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS'))
       OR(TH.transfer_date >= to_date(:TS_STARTD,'DD/MM/YYYY HH24:MI:SS') AND 
          TH.transfer_date <  to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS'))   )
INNER JOIN MTX_Transaction_items TI ON TH.transfer_id = TI.transfer_id AND TI.transaction_type = 'MR'
INNER JOIN MTX_Unreg_User UU ON UU.unreg_user_id = TI.unreg_user_id

ORDER   BY  custno 
)
