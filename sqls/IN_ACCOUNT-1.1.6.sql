
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
    , nvl(u.network_code, mp.network_code) as network_code
    , mw.wallet_number
    , mw.modified_on as wallet_modified_on
    , mw.created_on as wallet_created_on
    ,spms.subtype_name as wallet_type_name
    ,mcp.max_balance/100 as wallet_maximum_balance
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
INSTITUTE	||
CUSTNO	||
BUSINESSTYPE	||
ACCNO	||
BUSINESSNO	||
ACC_CURRENCYISO	||
ACCOPENING	||
ACCCLOSE	||
ACCHOLD_INSTITUTE	||
ACCHOLDCUSTNO	||
ACCLIMIT	||
ACCBALANCE	||
SUMCREDRUNYEAR	||
SUMDEBRUNYEAR	||
NUMBERACCOUNTS	||
CUST_FLAGS	||
PODTYPE	||
IBAN	||
ACC_TYPE	||
HOLD_MAIL	||
EMPLNO	||
PURPOSE	||
CONTR_DATINCEPT	||
CONTR_ENDDATE	||
TARIFF	||
PAYM_PERIOD	||
PAYM_MODE	||
PAYM_TYPE	||
TYPE_ACQUIS	||
LIFE_INCREASEYN	||
LIFE_LENTYN	||
CONTR_STATUS	||
INTERMED_TYPE	||
FK_CSMNO	||
BIC_PREMPAYOR	
FROM 
   (
SELECT 
   RPAD(:INSTITUTE,4)                            		institute,              
       RPAD(substr(u.user_type,1,1)||nvl(u.msisdn,' '),16)  					custno,                  
       RPAD(substr(u.wallet_number,1,2),4)                  					businesstype,            
       RPAD(substr(u.wallet_number,3,9),11)                    					accno,                   
       RPAD(substr(u.wallet_number,12,9),11)                   					businessno,              
       RPAD(u.currency_iso,3)            										acc_currencyiso,         
       TO_CHAR(u.wallet_created_on,'YYYYMMDD')    									accopening,              
       RPAD(decode(u.wallet_status,'N',TO_CHAR(u.wallet_modified_on,'YYYYMMDD'),' '),8)		accclose,                
       RPAD(' ',4)	                     										acchold_institute,       
       RPAD(' ',16)                    											accholdcustno,           
       TO_CHAR(u.wallet_maximum_balance,'S0000000000000.00')     					acclimit,                
       RPAD(' ',17)     														accbalance,              
       RPAD(' ',17)     														sumcredrunyear,          
       RPAD(' ',17)     														sumdebrunyear,           
       RPAD(' ',4)                   											numberaccounts,          
       RPAD(' ',24)                                								cust_flags,            
       RPAD(' ',5)	                     										podtype,                 
       RPAD(' ',35)			                    								iban,                    
       ' ' 			                               								acc_type,                
       ' ' 		                                								hold_mail,               
       RPAD(' ',16)			                    								emplno,                  
       RPAD(u.wallet_type_name,32)                    							purpose,                 
       RPAD(' ',8)			                     								contr_datincept,         
       RPAD(' ',8)			                     								contr_enddate,           
       RPAD(' ',32)			                    								tariff,                  
       RPAD(' ',4)								 			                    paym_period,             
       RPAD(' ',2)											                    paym_mode,               
       ' '			                                 							paym_type,               
       RPAD(' ',2)				                     							type_acquis,             
       ' '		                                								life_increaseyn,         
       ' '		                                								life_lentyn,             
       RPAD(' ',32)				                    							contr_status,            
       RPAD(' ',5)				                   								intermed_type,           
       RPAD(' ',12)				                    							fk_csmno,                
       RPAD(' ',12)				                    							bic_prempayor            
	   
	   FROM (
				SELECT /*+ FULL(u) */
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
