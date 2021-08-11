CREATE OR REPLACE FUNCTION ip_cluster(class_number integer) RETURNS  setof clustering_ip
AS $$
	query_all =  plpy.execute("with log as (select ip from ip_port group by ip order by sum(traffic) desc limit 100 )select ip_port.* from ip_port  join log  using(ip)");
	
	import pandas as pd
	import numpy as np
	df =  pd.DataFrame(query_all[0:],columns=["ip","port","udp_percentage","tcp_percentage" ,"icmp_percentage","http_percentage" ,"https_percentage","dns_percentage","quic_percentage", "traffic", "hits"]);
	
	df = df.fillna(0)
	df['ip'] = df['ip'].astype('category')
	df['port'] =  df['port'].astype('category')
	
	from sklearn.preprocessing import LabelEncoder
	labelencoder = LabelEncoder()
	df['port_cat'] = labelencoder.fit_transform(df['port'])
	df['ip_cat'] = labelencoder.fit_transform(df['ip'])
	
	from sklearn.preprocessing import OneHotEncoder 
	enc = OneHotEncoder(handle_unknown='ignore')
	enc_port = pd.DataFrame(enc.fit_transform(df[['port_cat','ip_cat']]).toarray())
	
	df = df.join(enc_port)
	df.drop('ip_cat',inplace=True,axis=1)
	
	X = df.iloc[:, 2:].values
	from sklearn.preprocessing import StandardScaler
	sc_X = StandardScaler()
	X_std = sc_X.fit_transform(X)
	
	from sklearn.cluster import AgglomerativeClustering
	cluster = AgglomerativeClustering(n_clusters=class_number, affinity='euclidean', linkage='ward')  
	class_list = cluster.fit_predict(X_std)
	
	re = []
	for i in range(len(query_all)):
		ip    = query_all[i]["ip"]
		label = class_list[i] 
		re.append(query_all[i])
		re[i]['class'] = label
	return re		
$$ LANGUAGE plpython3u;

CREATE FUNCTION subnet(net inet, mask0 int ) RETURNS  setof inet AS $$
DECLARE
	mask int;
	net1 inet;
 --rec int;
BEGIN
	mask = masklen(net);
	if mask < mask0 then 
	for i in 0..power(2, (mask0 - mask))-1 loop
		net1= host(net)::inet +(i * power(2,(32-mask0)))::int;
    	RETURN QUERY VALUES (set_masklen(net1,mask0));
	end loop;
	else 
	RETURN QUERY VALUES (net);
	end if;
END $$ language plpgsql;

CREATE OR REPLACE FUNCTION find_factor_2( n int ) RETURNS int[] AS $$
DECLARE
 i int;
 p int;
 a int ;
 out int[];
 res int[];
BEGIN
	i = n;
	if i=0 then
	for i in 1..7 loop
		res = res || i;
		end loop;
	return res;
	else	
	while i > 0 loop
		p = floor(log(2,i));
		out = p || out;
		i = i - power(2,p)::int ;
	end loop;
	
	res = res || out[1] ;
	for i in 2..array_length(out,1) loop
		for j in (out[i-1]+1)..out[i]-1 loop
			res = res || j;
			--raise notice 'test %',j;
			end loop;
		end loop;
		--elsif a=1 then 
	end if;	
	
	for i in out[array_upper(out,1)]+1..7 loop
		res = res || i;
		end loop;
	return res;
end;
$$ LANGUAGE plpgsql;

select find_factor_2(0)
		
 
CREATE OR REPLACE FUNCTION subnet_num( net1 inet,net2 inet)  RETURNS int[][] AS $$
DECLARE
num int ;
octet_num int ; 
diff int[];
last_subnet_value int;
i int;
ip_value1 text[];
ip_value2 text[];
res int[][];
BEGIN
ip_value1 = string_to_array(host(net1), '.');
ip_value2 = string_to_array(host(net2), '.');

for i in  1..4 loop
	diff =  diff  || (ip_value2[i]::int - ip_value1[i]::int);
	end loop;
--raise notice 'diff %',diff;
<<loop2>>	
for i in  1..4 loop	
	num = ip_value1[i]::int;
	last_subnet_value = ip_value2[i]::int;
	octet_num = i;
res = res|| array[[ num , last_subnet_value ,octet_num , (last_subnet_value - num)]]; 
end loop;
--diff = (net2 - net1)/256;
return  res ;
END;
$$ LANGUAGE plpgsql;

select subnet_num('1.3.0.0'::inet,'1.5.255.255'::inet)

-- FUNCTION: public.block(inet)

-- DROP FUNCTION public.block(inet);

CREATE OR REPLACE FUNCTION public.block(
	net inet)
    RETURNS TABLE(ips inet, distances double precision, class integer) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE
ip_list inet[];
distance_list double precision[];
step double precision;
center_ip double precision;
distance_MAX double precision;
block int[];
j int;
BEGIN
with log as (select ip, array_agg(port) as ports,
array_agg(array[
udp_percentage,
tcp_percentage ,  
icmp_percentage,
http_percentage ,
https_percentage,
dns_percentage,
quic_percentage, 
traffic, 
hits])  as featuers
from ip_port_standard where ip = net  group by ip),

log2 as (select ip, array_agg(port) as ports1,
array_agg(array[
udp_percentage,
tcp_percentage ,  
icmp_percentage,
http_percentage ,
https_percentage,
dns_percentage,
quic_percentage, 
traffic, 
hits]) as featuers1, (select  ports from log) as port2 , (select featuers from log) as featuers2
from ip_port_standard   group by ip ),

log3 as (select ip,  public.distance(ports1,featuers1,port2,featuers2) as dist from log2
order by 2 asc )

select array(select ip from log3),
array(select dist from log3), (select Max(dist) from log3)
into ip_list,distance_list,distance_MAX;

step = distance_MAX / array_length(ip_list,1);

j = 1;

center_ip = distance_list[1];
for i in 1..246978 loop
	if distance_list[i] - center_ip <= step then 
		block[i] = j;
	else 
		center_ip = distance_list[i];
		j = j+1;
		block[i] = j;
	end if;	
end loop;

for  i in  1..array_length(block,1) loop
ips := ip_list[i];
distances = distance_list[i];
class := block[i];
RETURN NEXT;
end loop;

raise notice 'step is calculated based on Max distance';
raise notice 'step : %',step;
raise notice 'number of cluster: %', j;

END
$BODY$;

ALTER FUNCTION public.block(inet)
    OWNER TO postgres;

-- FUNCTION: public.distance(bigint[], double precision[], bigint[], double precision[])

-- DROP FUNCTION public.distance(bigint[], double precision[], bigint[], double precision[]);

CREATE OR REPLACE FUNCTION public.distance(
	port1 bigint[],
	featuers1 double precision[],
	port2 bigint[],
	featuers2 double precision[])
    RETURNS SETOF double precision 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
DECLARE
dis float;
res float;
c int;
len1 int;
len2 int;
constant_dist float;
same_port_indices int[][];
BEGIN
res= 0;
c= 0;

for i in 1..array_length(port1,1) loop
	for j in 1..array_length(port2,1) loop
		if port1[i] = port2[j] then
			same_port_indices = same_port_indices || array[array[i,j]];
			c = c+1;
		end if;
	end loop;
end loop;

if c = 0 then	
for i in 1..array_length(featuers1,1) loop
 for q in 1..9 loop
	res = res + power((featuers1[i][q] - 0),2)::float;
	end loop;
end loop;

for k in 1..array_length(featuers2,1) loop
 for q in 1..9 loop
	res = res + power((featuers2[k][q] - 0),2)::float;
	end loop;
end loop;
dis = sqrt(res);

else
dis = euclidean_distance(featuers1,featuers2,same_port_indices) ; 	
end if;

RETURN QUERY VALUES (dis);
END
$BODY$;

ALTER FUNCTION public.distance(bigint[], double precision[], bigint[], double precision[])
    OWNER TO postgres;
----------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.inet_merge_py(
	nets inet[])
    RETURNS inet[]
    LANGUAGE 'plpython3u'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
from netaddr import cidr_merge;
res = cidr_merge(nets);
return res;
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.supernetting(
	input_table text,
	key_field text,
	param text)
    RETURNS TABLE (asn  text,
		organization_name text,
                network   inet)
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
BEGIN
RETURN QUERY EXECUTE 'select ' || key_field||' , unnest(inet_merge_py(array_agg(' || quote_ident(param)||')))::inet as network from ' 
|| quote_ident(input_table) || ' group by ' || key_field; 
END
$BODY$;

select * from supernetting('ip2location_asn', 'asn::text, as_name::text', 'cidr')
-------------------------------------------------------------------------------------

