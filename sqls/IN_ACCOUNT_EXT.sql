/*
** SCRIPT SQL : IN_ACCOUNT_EXT-1.1.5.sql
** VERSION    : 1.1.5
** DATE       : 10/09/2019					 
*/

/*list of actives users the last 91 Days before the start date */
WITH list_actives_91_Days AS(

	SELECT /*+ FULL(ti) */ distinct ti.party_id, WALLET_NUMBER
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
	SELECT /*+ FULL(ti) */ distinct ti.PARTY_ID,WALLET_NUMBER
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
	SELECT /*+ FULL(ti) */ distinct ti.PARTY_ID,WALLET_NUMBER
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
), 

mv_users_data as (
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
    , nvl(u.network_code, mp.network_code) as network_code
    , mw.wallet_number
    , mw.modified_on as wallet_modified_on
    , mw.created_on as wallet_created_on
    , spms.subtype_name as wallet_type_name
    , mcp.max_balance/100 as wallet_maximum_balance
	, mw.payment_type_id as wallet_type_id
	, cg.grade_name
from mtx_wallet mw
    left join mtx_party mp on (mw.user_id = mp.user_id and mw.user_type = mp.user_type)
    left join users u on (mw.user_id = u.user_id and mw.user_type <> 'SUBSCRIBER')
    left join mtx_categories mc on (mc.category_code = nvl(mp.category_code, u.category_code))
    left join mtx_trf_cntrl_profile mcp on (mw.mpay_profile_id = mcp.profile_id)
    left join channel_grades cg on (mcp.grade_code = cg.grade_code and cg.status = 'Y')
    left join sys_payment_method_subtypes spms on (mw.payment_type_id = spms.payment_type_id)
where mw.user_type = 'OPERATOR' or mp.user_id is not null or u.user_id is not null
)

SELECT 
'"'||	institute	||		'"|'||
'"'||	custno		||		'"|'||
'"'||	businesstype	||		'"|'||
'"'||	accno		||		'"|'||
'"'||	businessno	||		'"|'||
'"'||	acc_currencyiso ||		'"|'||
'"'||	acc_sph_01	||		'"|'||
'"'||	acc_sph_02	||		'"|'||
'"'||	acc_sph_03	||		'"|'||
'"'||	acc_sph_04	||		'"|'||
'"'||	acc_sph_05	||		'"|'||
'"'||	acc_sph_06	||		'"|'||
'"'||	acc_sph_07	||		'"|'||
'"'||	acc_sph_08	||		'"|'||
'"'||	acc_sph_09	||		'"|'||
'"'||	acc_sph_10	||		'"|'||
'"'||	acc_sph_11	||		'"|'||
'"'||	acc_sph_12	||		'"|'||
'"'||	acc_sph_13	||		'"|'||
'"'||	acc_sph_14	||		'"|'||
'"'||	acc_sph_15	||		'"|'||
'"'||	acc_sph_16	||		'"|'||
'"'||	acc_sph_17	||		'"|'||
'"'||	acc_sph_18	||		'"|'||
'"'||	acc_sph_19	||		'"|'||
'"'||	acc_sph_20	||		'"|'||
'"'||	acc_sph_21	||		'"|'||
'"'||	acc_sph_22	||		'"|'||
'"'||	acc_sph_23	||		'"|'||
'"'||	acc_sph_24	||		'"|'||
'"'||	acc_sph_25	||		'"|'||
'"'||	acc_sph_26	||		'"|'||
'"'||	acc_sph_27	||		'"|'||
'"'||	acc_sph_28	||		'"|'||
'"'||	acc_sph_29	||		'"|'||
'"'||	acc_sph_30	||		'"|'||
'"'||	acc_sph_31	||		'"|'||
'"'||	acc_sph_32	||		'"|'||
'"'||	acc_sph_33	||		'"|'||
'"'||	acc_sph_34	||		'"|'||
'"'||	acc_sph_35	||		'"|'||
'"'||	acc_sph_36	||		'"|'||
'"'||	acc_sph_37	||		'"|'||
'"'||	acc_sph_38	||		'"|'||
'"'||	acc_sph_39	||		'"|'||
'"'||	acc_sph_40	||		'"|'||
'"'||	acc_sph_41	||		'"|'||
'"'||	acc_sph_42	||		'"|'||
'"'||	acc_sph_43	||		'"|'||
'"'||	acc_sph_44	||		'"|'||
'"'||	acc_sph_45	||		'"|'||
'"'||	acc_sph_46	||		'"|'||
'"'||	acc_sph_47	||		'"|'||
'"'||	acc_sph_48	||		'"|'||
	acc_nph_01	||		 '|'||
	acc_nph_02	||		 '|'||
	acc_nph_03	||		 '|'||
	acc_nph_04	||		 '|'||
	acc_nph_05	||		 '|'||
	acc_nph_06	||		 '|'||
	acc_nph_07	||		 '|'||
	acc_nph_08	||		 '|'||
	acc_nph_09	||		 '|'||
	acc_nph_10		
FROM 
   (
SELECT 
		RPAD(:INSTITUTE,4)                        institute,              
       substr(u.user_type,1,1)||nvl(u.msisdn,' ')   					custno,                
       substr(u.wallet_number,1,2)                  					businesstype,          
       substr(u.wallet_number,3,9)                    					accno,                 
       substr(u.wallet_number,12,9)                    					businessno,            
       SUBSTR(u.currency_iso,1,3)            							acc_currencyiso,       
       u.user_id														acc_sph_01,				
	   u.wallet_type_id													acc_sph_02,				
	   u.grade_name													acc_sph_03,				
	   	null									acc_sph_04,
		null									acc_sph_05,
		null									acc_sph_06,
		null									acc_sph_07,
		null									acc_sph_08,
		null									acc_sph_09,
		null									acc_sph_10,
		null									acc_sph_11,
		null									acc_sph_12,
		null									acc_sph_13,
		null									acc_sph_14,
		null									acc_sph_15,
		null									acc_sph_16,
		null									acc_sph_17,
		null									acc_sph_18,
		null									acc_sph_19,
		null									acc_sph_20,
		null									acc_sph_21,
		null									acc_sph_22,
		null									acc_sph_23,
		null									acc_sph_24,
		null									acc_sph_25,
		null									acc_sph_26,
		null									acc_sph_27,
		null									acc_sph_28,
		null									acc_sph_29,
		null									acc_sph_30,
		null									acc_sph_31,
		null									acc_sph_32,
		null									acc_sph_33,
		null									acc_sph_34,
		null									acc_sph_35,
		null									acc_sph_36,
		null									acc_sph_37,
		null									acc_sph_38,
		null									acc_sph_39,
		null									acc_sph_40,
		null									acc_sph_41,
		null									acc_sph_42,
		null									acc_sph_43,
		null									acc_sph_44,
		null									acc_sph_45,
		null									acc_sph_46,
		null									acc_sph_47,
		null									acc_sph_48,
		to_number(null)							acc_nph_01,
		to_number(null)							acc_nph_02,
		to_number(null)							acc_nph_03,
		to_number(null)							acc_nph_04,
		to_number(null)							acc_nph_05,
		to_number(null)							acc_nph_06,
		to_number(null)							acc_nph_07,
		to_number(null)							acc_nph_08,
		to_number(null)							acc_nph_09,
		to_number(null)							acc_nph_10
		FROM (SELECT /*+ FULL(u) */
				u.*, i.currency_iso
				FROM mv_users_data u 
					inner join list_actives_last_90_days on list_actives_last_90_days.PARTY_ID = u.user_id and list_actives_last_90_days.wallet_number = u.WALLET_NUMBER
					left join imt_country_code i on i.country_iso = u.network_code 
				WHERE  
					--:TYPOFFILE = 'FULLACTIVE' AND 
					u.user_type in ('SUBSCRIBER','CHANNEL')
					
			UNION  
			
				SELECT /*+ FULL(u) */
				u.*, i.currency_iso
				FROM mv_users_data u 
					inner join (SELECT L2.PARTY_ID , L2.WALLET_NUMBER
						FROM list_actives_91_Days  L1
							RIGHT JOIN list_actives_Of_Day L2 ON (L1.party_id = L2.PARTY_ID and L1.WALLET_NUMBER = L2.WALLET_NUMBER )
						WHERE 
							L1.party_id is null) List_reactives on List_reactives.PARTY_ID = u.user_id and List_reactives.WALLET_NUMBER = u.WALLET_NUMBER
					left join imt_country_code i on i.country_iso = u.network_code 
				WHERE  --:TYPOFFILE = 'DELTA'	AND 
					u.status <> 'N'
					AND u.user_type in ('SUBSCRIBER','CHANNEL')
					AND	u.wallet_created_on < to_date(:TS_STARTD,'DD/MM/YYYY HH24:MI:SS') - 91
					AND u.wallet_status <> 'N' 
			UNION 
			
				Select 	/*+ FULL(u) */
				u.*, i.currency_iso
				FROM    mv_users_data u 
						left join imt_country_code i on i.country_iso = u.network_code 
				WHERE   ((u.wallet_created_on   >=  to_date(:TS_STARTD,'DD/MM/YYYY HH24:MI:SS') AND u.wallet_created_on   <  to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS'))
					OR	(u.wallet_modified_on  >=  to_date(:TS_STARTD,'DD/MM/YYYY HH24:MI:SS') AND u.wallet_modified_on  <  to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS')))
						AND u.user_type in ('CHANNEL','SUBSCRIBER')
						and (:TYPOFFILE <> 'FULL' or (u.status <> 'N' and u.wallet_status <> 'N') )
						AND ( U.MSISDN = :MSISDN OR 'ALL' =  :MSISDN )
			) u
		ORDER   BY  
            custno,         
            businesstype,   
            accno,          
            businessno,     
            acc_currencyiso
) 
