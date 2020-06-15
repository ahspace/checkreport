select CASE WHEN
((extract(second from to_dsinterval(value)) + extract(minute from to_dsinterval(value)) * 60 
 + extract(hour from to_dsinterval(value)) *60*60 + extract(day from to_dsinterval(value)) *60*60*24)
> 1800) THEN 'DELAY'
WHEN value is null THEN ' Critical Data Broken status'
WHEN (((sysdate - to_date(DATUM_TIME,'MM/DD/YYYY HH24:MI:SS'))*24*60) > 30) THEN 'DELAY'
ELSE 'OK'
END as Sync_status, '' || value
from v$dataguard_stats@PO<country_code>T.FT where name='apply lag'