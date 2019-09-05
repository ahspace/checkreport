select
to_char(id,'FM000') COUNTRYINT,
decode(description,'Panama','PA ',rpad(country_iso,3)) COUNTRYEXT,
rpad(description,128)   DESCR
from IMT_COUNTRY_CODE
where status = 'Y'
and description <> 'Guadeloupe'
order by COUNTRYEXT asc
