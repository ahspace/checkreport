SELECT case when to_date(substr(value,4),'HH24:MI:SS')>to_date('00:30:00','HH24:MI:SS') 
            then 'DELAY' 
            else 'OK'
            end as DBREF_SYNC_STATUS, '' || value
  FROM (
	select nvl2(p.applied_time,(p.latest_time-p.applied_time) day(2) to second(0) ,(sysdate-max(al.first_time)) day(2) to second(0) ) as value
	from v$logstdby_progress p
	left outer join dba_logstdby_log al  on (al.applied in ('YES','CURRENT') and p.restart_scn between al.first_change# and al.next_change#)
	group by p.applied_time,p.latest_time
     )
