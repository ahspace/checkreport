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
	SELECT /*+ FULL(ti) */ distinct ti.party_id, WALLET_NUMBER
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
	SELECT /*+ FULL(ti) */ distinct ti.party_id, WALLET_NUMBER
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
mv_users as (
select mw.user_id, mw.is_primary wallet_primary
    , mw.status wallet_status
    , nvl(mp.status, u.status) status
    , nvl(mp.user_type, u.user_type) user_type
	, mw.modified_on as wallet_modified_on
    , mw.created_on as wallet_created_on
	, mw.wallet_number
	, nvl(mp.msisdn,u.msisdn) msisdn
    from mtx_wallet mw
    left join mtx_party mp on mw.user_id = mp.user_id 
    left join users u on mw.user_id = u.user_id 
	where mw.user_type = 'CHANNEL' or  mw.user_type = 'SUBSCRIBER'
    )

select count(*) from (
	select u.user_id, u.wallet_number from mv_users u
        where ((u.wallet_created_on >= to_date(:TS_STARTD,'DD/MM/YYYY HH24:MI:SS') and u.wallet_created_on < to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS') )
            or (u.wallet_modified_on >= to_date(:TS_STARTD,'DD/MM/YYYY HH24:MI:SS') and u.wallet_modified_on < to_date(:TS_CURD,'DD/MM/YYYY HH24:MI:SS') ))
        and (u.MSISDN = :MSISDN or 'ALL' = :MSISDN)
		
	union
	
	select L.party_id user_id, L.WALLET_NUMBER from list_actives_last_90_days L
	
	union 
	
	select List_reactives.user_id, List_reactives.WALLET_NUMBER
	FROM mv_users u 
					inner join (
	SELECT L2.PARTY_ID user_id, L2.WALLET_NUMBER
		FROM list_actives_91_Days  L1
		RIGHT JOIN list_actives_Of_Day L2 ON (L1.party_id = L2.PARTY_ID and L1.WALLET_NUMBER = L2.WALLET_NUMBER )
		WHERE 
			L1.party_id is null) List_reactives on List_reactives.USER_ID = u.user_id and List_reactives.wallet_number = u.wallet_number
				WHERE u.status <> 'N'
					AND	u.wallet_created_on < to_date(:TS_STARTD,'DD/MM/YYYY HH24:MI:SS') - 91
					AND u.wallet_status <> 'N')
