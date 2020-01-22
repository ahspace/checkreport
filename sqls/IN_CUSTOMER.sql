/*
** SCRIPT SQL : IN_CUSTOMER-1.2.0.sql
** VERSION    : 1.2.0
** DATE       : 25/05/2018
** MODIFIED DATE: 21/08/2018 
** DESCRIPTION: Change regarding Performance improvement
•	Uses bind variables, instead of substitution variables => stable/same SQL_ID 
•	Uses raw data (no temporary tables are used) => it can be executed at any moment without waiting for temporary tables to finish
19/11/2019 : Remove inactive users in fullactive mode
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
    , nvl(u.network_code, mp.network_code) as network_code
from mtx_wallet mw
    left join mtx_party mp on (mw.user_id = mp.user_id and mw.user_type = mp.user_type)
    left join users u on (mw.user_id = u.user_id and mw.user_type <> 'SUBSCRIBER')
    left join mtx_categories mc on (mc.category_code = nvl(mp.category_code, u.category_code))
    left join mtx_trf_cntrl_profile mcp on (mw.mpay_profile_id = mcp.profile_id)
    left join channel_grades cg on (mcp.grade_code = cg.grade_code and cg.status = 'Y')
    left join sys_payment_method_subtypes spms on (mw.payment_type_id = spms.payment_type_id)
where mw.user_type = 'OPERATOR' or mp.user_id is not null or u.user_id is not null
),
 list_actives_91_Days AS(

	SELECT /*+ FULL(ti) */ distinct ti.party_id , ti.ACCOUNT_ID msisdn
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
select institute
	|| custno
	|| firstname
	|| lastname
	|| street
	|| zip
	|| town
	|| h_countryflag
	|| s_countryflag
	|| cusy
	|| fk_csmno
	|| profession
	|| branch
	|| birthdate
	|| custcontact
	|| exemptionflag
	|| exemptionamount
	|| asylsyn
	|| salary
	|| salarydate
	|| nat_countryflag
	|| tot_wealth
	|| prop_wealth
	|| branch_office
	|| cust_type
	|| cust_flags
	|| emplno
	|| pass_no
	|| birth_country
	|| birth_place
	|| borroweryn
	|| direct_debityn
	|| gender
	|| risk_class as line
from (select distinct rpad(:INSTITUTE,4) institute
        , rpad(substr(u.user_type,1,1) || nvl(u.msisdn,' '),16) custno
        , rpad(nvl(u.user_name,' '),32) firstname
        , rpad(nvl(trim(u.last_name),'UNKNOWN'),32) lastname
        , rpad(nvl(u.address1,' '),32) street
        , rpad(' ',7) zip
        , rpad(nvl(u.city,' '),28) town
        , case
            when (select count(1)
              from imt_country_code
              where country_iso        = u.addon_homecountry
              and u.addon_homecountry is not null
              and status               = 'Y') > 0
            then rpad(nvl(u.addon_homecountry,' '),3)
            else rpad(' ',3)
          end as h_countryflag
        , rpad(' ',3) s_countryflag
        , rpad(' ',8) cusy
        , rpad(' ',12) fk_csmno
        , rpad(nvl(u.user_mark,' '),32) profession
        , rpad(nvl(u.user_type,' '),32) branch
        , decode(rpad(nvl(to_char(u.dob,'YYYYMMDD'),' '),8),'00000000','19000101',rpad(nvl(to_char(u.dob,'YYYYMMDD'),' '),8)) birthdate
        , decode(rpad(nvl(to_char(u.created_on,'YYYYMMDD'),' '),8),'00000000','19000101',rpad(nvl(to_char(u.created_on,'YYYYMMDD'),' '),8)) custcontact
        , ' ' exemptionflag
        , rpad(' ',11) exemptionamount
        , ' ' asylsyn
        , rpad(' ',17) salary
        , rpad(' ',8) salarydate
        , case
            when (select count(1)
              from imt_country_code
              where country_iso        = u.addon_nationality
              and u.addon_nationality is not null
              and status               = 'Y') > 0
            then rpad(nvl(u.addon_nationality,' '),3)
            else rpad(' ',3)
          end as nat_countryflag
        , rpad(' ',17) tot_wealth
        , rpad(' ',3) prop_wealth
        , rpad(' ',10) branch_office
        , ' ' cust_type
        , rpad(' ',24) cust_flags 
        , rpad(nvl(u.agent_code,' '),16) emplno
        , rpad(' ',17) pass_no
        , rpad(' ',3) birth_country
        , rpad(' ',32) birth_place
        , ' ' borroweryn
        , ' ' direct_debityn
        , nvl(substr(u.gender,5,1),' ') gender
        , rpad(' ',10) risk_class
    from (
				SELECT /*+ FULL(u) */
				u.*
				FROM mv_users_data u 
					inner join list_actives_last_90_days on list_actives_last_90_days.PARTY_ID = u.user_id 
						and list_actives_last_90_days.msisdn = u.msisdn
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
							RIGHT JOIN list_actives_Of_Day L2 ON (L1.party_id = L2.PARTY_ID and L1.msisdn = L2.msisdn)
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
			
				Select /*+ FULL(u) */ *
				FROM    mv_users_data u 
				WHERE   u.status <> 'N'
					AND u.user_type in ('SUBSCRIBER','CHANNEL')
					AND u.wallet_primary = 'Y'
					AND u.wallet_status <> 'N'
					AND (   (u.creation_date      >=  to_date(:TS_STARTD,'DD/MM/YYYY HH24:MI:SS') AND u.creation_date      <  to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS'))            
						 OR (u.modification_date  >=  to_date(:TS_STARTD,'DD/MM/YYYY HH24:MI:SS') AND u.modification_date  <  to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS')))
					AND ( U.MSISDN = :MSISDN  OR 'ALL' =  :MSISDN  )
					and (:TYPOFFILE <> 'FULL' or (u.status <> 'N' and u.wallet_status <> 'N' and u.wallet_primary ='Y') )
			) u	
			
    union all
	
    select distinct rpad(:INSTITUTE,4) institute
        , rpad('U' || nvl(uu.msisdn,' '),16) custno
        , rpad(nvl(rk.rec_fname,' '),32) firstname
        , rpad(nvl(trim(rk.rec_lname),'UNKNOWN'),32) lastname
        , rpad(' ',32) street
        , rpad(' ',7) zip
        , rpad(' ',28) town
        , rpad(' ',3) h_countryflag
        , rpad(' ',3) s_countryflag
        , rpad(' ',8) cusy
        , rpad(' ',12) fk_csmno
        , rpad(' ',32) profession
        , rpad('UNREGISTERED',32) branch
        , DECODE(rpad(nvl(TO_CHAR(rk.rec_dob,'YYYYMMDD'),' '),8),'00000000','19000101',rpad(nvl(TO_CHAR(rk.rec_dob,'YYYYMMDD'),' '),8)) birthdate
        , '        ' custcontact
        , ' ' exemptionflag
        , rpad(' ',11) exemptionamount
        , ' ' asylsyn
        , rpad(' ',17) salary
        , rpad(' ',8) salarydate
        , '   ' nat_countryflag
        , rpad(' ',17) tot_wealth
        , rpad(' ',3) prop_wealth
        , rpad(' ',10) branch_office
        , ' ' cust_type
        , rpad(' ',24) cust_flags 
        , rpad(' ',16) emplno
        , rpad(' ',17) pass_no
        , rpad(' ',3) birth_country
        , rpad(' ',32) birth_place
        , ' ' borroweryn
        , ' ' direct_debityn
        , ' ' gender
        , rpad(' ',10) risk_class
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
        inner join mtx_unreg_user uu on uu.unreg_user_id = tx.unreg_user_id)
order by custno
