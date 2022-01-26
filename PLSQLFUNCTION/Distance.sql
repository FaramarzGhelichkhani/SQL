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
