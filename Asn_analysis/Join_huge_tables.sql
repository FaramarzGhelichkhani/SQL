WITH LOG AS (select *
,case when db_name = 'ip2location' then cidr else null end as block
,sum(case when (case when db_name = 'ip2location' then cidr else null end) is null then 0 else 1 end) over (order by host(cidr)::inet,(case when db_name = 'ip2location' then cidr else null end)) as block_value 
from geolocation
where db_name <> 'geodb'
order by host(cidr)::inet,block,block_value  ),
log2 as(
SELECT db_name,asn ,cidr,description,block,block_value,
case 
	when cidr = first_value(block) over (partition by block_value) then 'equals' 
	when cidr << first_value(block) over (partition by block_value) then 'is contained within' 
	when cidr >> first_value(block) over (partition by block_value) then 'contains' 
	end as cidr_status
, case 
	when asn = first_value(asn) over (partition by block_value) then true 
	else False 
	end as asn_status
,case 
	when cidr <<= any(array['0.0.0.0/8','10.0.0.0/8','100.64.0.0/10','127.0.0.0/8','169.254.0.0/16','172.16.0.0/12','192.0.0.0/24','192.0.0.0/29',
'192.0.0.8/32','192.0.0.9/32','192.0.0.10/32','192.0.0.170/32','192.0.2.0/24','192.31.196.0/24','192.52.193.0/24',
'192.88.99.0/24','192.168.0.0/16','192.175.48.0/24','198.18.0.0/15','198.51.100.0/24','203.0.113.0/24','240.0.0.0/4','255.255.255.255/32']::inet[])
	then 'reserved_ip' else 'public_ip' end as ip_status
FROM LOG 
order by  host(cidr)::inet,block,block_value
)
-- insert into asn_status(db_name,asn ,cidr,description,block,block_number,cidr_status,asn_status,ip_status)
select * from log2
