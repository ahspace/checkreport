/*
** SCRIPT SQL : IN_COUNTRY.sql
**
	18/11/2019 update after fixing the country code for Panama and Guadeloupe - Marie Rakotovao
*/
select
to_char(id,'FM000') COUNTRYINT,
rpad(country_iso,3) COUNTRYEXT,
rpad(description,128) DESCR
from IMT_COUNTRY_CODE
where status = 'Y'
order by COUNTRYEXT asc
