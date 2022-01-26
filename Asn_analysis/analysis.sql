with log as (select block_number , count(cidr) as cidrs,count(distinct db_name) filter (where asn_status) as istrue,count(distinct db_name) filter (where not asn_status) as isfalse 
from asn_status
			where family(cidr) =4
			group by block_number)
select istrue,isfalse , count(*) , count(*)*100 / (select count(*) from log) , (array_agg(block_number order by random()))[1:3]::text[] as block,
(array_agg(cidrs))[1]::text as cidrs
from log 
group by istrue,isfalse 
order by 2
asc
