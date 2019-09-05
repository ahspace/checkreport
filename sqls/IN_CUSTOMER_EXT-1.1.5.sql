/*
** SCRIPT SQL : IN_CUSTOMER_EXT-1.1.5.sql
** VERSION    : 1.1.5
** DATE       : 02/08/2018
** DESCRIPTION:  Change regarding Performance improvement
*	Uses bind variables, instead of substitution variables => stable/same SQL_ID 
*	Uses raw data (no temporary tables were used) => it can be executed at any moment without waiting for temporary tables to finish
*	Incuded Marie updates
*/
with 
mv_txn_header_as_of_date as (
select /*+ materialize */
        transfer_id
from mtx_transaction_header
where transfer_date < to_date(:TS_STARTD,'DD/MM/YYYY HH24:MI:SS')
    and modified_on >= to_date(:TS_STARTD,'DD/MM/YYYY HH24:MI:SS')
    and modified_on < to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS')
)
, mv_mtx_transaction_items as (
select transfer_id
    , unreg_user_id
from mtx_transaction_items
where transfer_date >= to_date(:TS_STARTD,'DD/MM/YYYY HH24:MI:SS')
    and transfer_date < to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS')
    and transaction_type = 'MR'
)
, mv_users_data as (
select
    decode(mw.user_type,'OPERATOR', mw.user_type, nvl(mp.user_type, u.user_type)) user_type
    , nvl2(mp.user_id, mp.msisdn, nvl2(u.user_id, u.msisdn, mw.msisdn)) msisdn
    , nvl(mp.user_name, u.user_name) user_name
    , nvl(mp.last_name, u.last_name) last_name
    , nvl(mp.address1, u.address1) address1
    , nvl(mp.city, u.city) city 
    , case 
        when regexp_like(mp.external_code, '^1[[:upper:]]{5}.+') then decode(substr(mp.external_code,5,2),'XX',null,substr(mp.external_code,5,2))
        else null
    end addon_homecountry
    , case 
        when regexp_like(mp.external_code, '^1[[:upper:]]{5}.+') then decode(substr(mp.external_code,3,2),'XX',null,substr(mp.external_code,3,2))
        else null
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
    ,mc.category_name as HEAD_CATEGORY_NAME
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
		   nvl((select idtype_label from dbref_idtypes where type_id = substr(mp.external_code,2,1)),'OTHER')
		 else
		  null
		 end as addon_idtype
     ,replace(replace(convert(mp.id_no, 'US7ASCII', 'WE8ISO8859P1'),chr(10),' '),chr(13),' ') as ID_NO
     ,case when REGEXP_LIKE(nvl(mp.external_code, u.external_code), '^1[[:upper:]]{5}.+') then
		   substr(mp.external_code,7)
			 else
		  replace(replace(convert(nvl(mp.external_code,u.external_code), 'US7ASCII', 'WE8ISO8859P1'),chr(10),' '),chr(13),' ')
		 end as external_code
     ,nvl(mp.address2,u.address2) as address2
	 , nvl(u.network_code, mp.network_code) as network_code
from mtx_wallet mw
    left join mtx_party mp on (mw.user_id = mp.user_id and mw.user_type = mp.user_type)
    left join users u on (mw.user_id = u.user_id and mw.user_type <> 'SUBSCRIBER')
    left join mtx_category_relations head_mcr on (head_mcr.to_category =nvl(mp.category_code, u.category_code)) and head_mcr.relation_type ='OWNER' and head_mcr.status='Y'
	left join mtx_categories mc on head_mcr.from_category = mc.category_code
    left join mtx_trf_cntrl_profile mcp on (mw.mpay_profile_id = mcp.profile_id)
    left join channel_grades cg on (mcp.grade_code = cg.grade_code and cg.status = 'Y')
    left join sys_payment_method_subtypes spms on (mw.payment_type_id = spms.payment_type_id)
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
			OR	(ti.transaction_type = 'MP'))
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
			OR	(ti.transaction_type = 'MP'))
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
			OR	(ti.transaction_type = 'MP'))
		AND ti.transfer_date >= to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS') - 90
		AND ti.transfer_date < to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS')
		AND ti.transfer_status = 'TS'
)

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
AS LINE
                    /*IN_CUSTOMER_EXT_CHA_SUB.sql*/
FROM  (
SELECT Distinct
        RPAD(:INSTITUTE,4)                            	institute,              
        substr(u.user_type,1,1)||nvl(u.msisdn,' ')    		custno,             -- 
        translate(SUBSTR(u.user_name,33),'"',' ')            			cust_sph_01,        -- 
        translate(SUBSTR(u.last_name,33),'"',' ')            			cust_sph_02,        -- 
        translate(SUBSTR(u.address1,33),'"',' ')   			cust_sph_03,        -- 
        translate(u.address2,'"',' ')          			cust_sph_04,        -- 
        translate(SUBSTR(u.city,29),'"',' ')      			cust_sph_05,        -- 
        translate(u.external_code,'"',' ')        		cust_sph_06,        --
        u.id_no                            			cust_sph_07,        -- 
        u.addon_idtype    						cust_sph_08,        --
        u.category_code                    			cust_sph_09,        -- 
	u.category_name						cust_sph_10, 
	decode(u.user_type,'CHANNEL',u.state,null)		cust_sph_11,
	null						cust_sph_12,
	null		cust_sph_13,	
	null					cust_sph_14,
	null					cust_sph_15,
	null				cust_sph_16,
	translate(SUBSTR(u.remarks,1,64),'"',' ')					cust_sph_17,
	translate(SUBSTR(u.remarks,65),'"',' ')					cust_sph_18,
	u.user_id						cust_sph_19,
	u.hierarchical_level					cust_sph_20,
	u.head_category_code					cust_sph_21,
	u.head_category_name					cust_sph_22,
	null								cust_sph_23,
	null								cust_sph_24,
	null								cust_sph_25,
	null								cust_sph_26,
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
		to_number(null)						cust_nph_01,
		to_number(null)						cust_nph_02,
		to_number(null)						cust_nph_03,
		to_number(null)						cust_nph_04,
		to_number(null)						cust_nph_05,
		to_number(null)						cust_nph_06,
		to_number(null)						cust_nph_07,
		to_number(null)						cust_nph_08,
		to_number(null)						cust_nph_09,
		to_number(null)						cust_nph_10
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
					AND	u.creation_date < to_date(:TS_STARTD,'DD/MM/YYYY HH24:MI:SS') - 91
					AND u.wallet_primary = 'Y'
					AND u.wallet_status <> 'N'
					
			UNION 
		
		Select /*+ FULL(u) */
			* 
			FROM    mv_users_data u 
			WHERE   u.status <> 'N'
				AND u.user_type in ('SUBSCRIBER','CHANNEL')
				AND u.wallet_primary = 'Y'
				AND u.wallet_status <> 'N' 
				AND	(   (u.creation_date  >=  to_date(:TS_STARTD,'DD/MM/YYYY HH24:MI:SS') AND u.creation_date   <  to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS'))	
					OR (u.modification_date  >=  to_date(:TS_STARTD,'DD/MM/YYYY HH24:MI:SS') AND u.modification_date  <  to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS')) )	
				and (:TYPOFFILE <> 'FULL' or (u.status <> 'N' and u.wallet_status <> 'N' and u.wallet_primary ='Y') )
				AND ( U.MSISDN = :MSISDN OR 'ALL' =  :MSISDN )
		) u
LEFT JOIN mv_users_data created on u.created_by = created.user_id
		
UNION ALL            /*IN_CUSTOMER_EXT_TNO.sql*/
SELECT Distinct
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
