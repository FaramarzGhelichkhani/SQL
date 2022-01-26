WITH RECURSIVE 
timesrc AS (
(SELECT date_trunc('day',time) AS time FROM log_stats_servertraffic1hour ORDER BY time DESC LIMIT 1)
UNION ALL
SELECT (
SELECT date_trunc('day',time) FROM log_stats_servertraffic1hour
WHERE time < t.time
ORDER BY time DESC LIMIT 1
)
FROM timesrc t
WHERE t.time IS NOT NULL
),
---------------------------- dst tables times  
timedest_Onewaytraffic1Day AS (
SELECT date_trunc('day',time) AS time FROM zafirmetadata_Onewaytraffic1Day ORDER BY time DESC LIMIT 1)
UNION ALL
SELECT (
SELECT date_trunc('day',time) FROM zafirmetadata_Onewaytraffic1Day
WHERE time < t.time
ORDER BY time DESC LIMIT 1
)
FROM timedest_Onewaytraffic1Day t
WHERE t.time IS NOT NULL
),
timedest_Anarestan1DayAS (
SELECT date_trunc('day',time) AS time FROM zafirmetadata_Anarestan1Day ORDER BY time DESC LIMIT 1)
UNION ALL
SELECT (
SELECT date_trunc('day',time) FROM zafirmetadata_Anarestan1Day
WHERE time < t.time
ORDER BY time DESC LIMIT 1
)
FROM timedest_Anarestan1DayAS t
WHERE t.time IS NOT NULL
),
timedest_Child1Day (
SELECT date_trunc('day',time) AS time FROM zafirmetadata_Child1Day ORDER BY time DESC LIMIT 1)
UNION ALL
SELECT (
SELECT date_trunc('day',time) FROM zafirmetadata_Child1Day
WHERE time < t.time
ORDER BY time DESC LIMIT 1
)
FROM timedest_Child1Day t
WHERE t.time IS NOT NULL
),
timedest_Education1Day (
SELECT date_trunc('day',time) AS time FROM zafirmetadata_Education1Day ORDER BY time DESC LIMIT 1)
UNION ALL
SELECT (
SELECT date_trunc('day',time) FROM zafirmetadata_Education1Day
WHERE time < t.time
ORDER BY time DESC LIMIT 1
)
FROM timedest_Education1Day t
WHERE t.time IS NOT NULL
),
---------------------------- aggregate times
times_Onewaytraffic1Day as (
(select *
from timesrc
where time  < date_trunc('day',now()
except
select *
from timedest_Onewaytraffic1Day
) order by time asc limit 2)
,
times_Anarestan1DayAS as (
(select *
from timesrc
where time  < date_trunc('day',now()
except
select *
from timedest_Anarestan1DayAS 
) order by time asc limit 2)
,
times_Child1Day as (
(select *
from timesrc
where time  < date_trunc('day',now()
except
select *
from timedest_Child1Day 
) order by time asc limit 2)
,
times_Education1Day  as (
(select *
from timesrc
where time  < date_trunc('day',now()
except
select *
from timedest_Education1Day 
) order by time asc limit 2)
---------------------------- insert data
insert into zafirmetadata_Onewaytraffic1Day(action_id, ip, time, domain_name, l7_proto, bsc, bcs, pcs, psc, hit, app_id, l4_proto_id, port)
select  action_id, ip ,
                date_trunc('day',time) as trunc ,
                domain_name, l7_proto, 
                sum(bsc) as bsc ,  
                sum(bcs) as bcs , sum(pcs) as pcs, sum(psc) as psc
                ,sum(hit) as hit 
                , app_id, l4_proto_id,port
                from log_stats_servertraffic1hour
                where  time >= (select time from times_Onewaytraffic1Day  order by time asc limit 1 )  AND time < (select time from times_Onewaytraffic1Day  order by time desc limit 1 ) + interval '1 day'
		and detection_history ~ ''
                group by ip, domain_name, trunc, l7_proto, l4_proto_id, app_id, action_id,port

insert into zafirmetadata_Anarestan1Day(action_id, ip, time, domain_name, l7_proto, bsc, bcs, pcs, psc, hit, app_id, l4_proto_id, port)
select  action_id, ip ,
                date_trunc('day',time) as trunc ,
                domain_name, l7_proto, 
                 sum(bsc) as bsc ,  
                sum(bcs) as bcs ,   sum(pcs) as pcs, sum(psc) as psc
                ,sum(hit) as hit 
                , app_id, l4_proto_id,port
                from log_stats_servertraffic1hour
                where  time >= (select time from times_Anarestan1Day  order by time asc limit 1 )  AND time < (select time from times_Anarestan1Day  order by time desc limit 1 ) + interval '1 day'
		and detection_history ~ '28101m'
                group by ip, domain_name, trunc, l7_proto, l4_proto_id, app_id, action_id,port


insert into zafirmetadata_Child1Day(action_id, ip, time, domain_name, l7_proto, bsc, bcs, pcs, psc, hit, app_id, l4_proto_id, port)
select  action_id, ip ,
                date_trunc('day',time) as trunc ,
                domain_name, l7_proto, 
                 sum(bsc) as bsc ,  
                sum(bcs) as bcs ,   sum(pcs) as pcs, sum(psc) as psc
                ,sum(hit) as hit 
                , app_id, l4_proto_id,port
                from log_stats_servertraffic1hour
                where  time >= (select time from times_Child1Day  order by time asc limit 1 )  AND time < (select time from times_Child1Day  order by time desc limit 1 ) + interval '1 day'
		and detection_history ~ '28117m'
                group by ip, domain_name, trunc, l7_proto, l4_proto_id, app_id, action_id,port

insert into zafirmetadata_Education1Day(action_id, ip, time, domain_name, l7_proto, bsc, bcs, pcs, psc, hit, app_id, l4_proto_id, port)
select  action_id, ip ,
                date_trunc('day',time) as trunc ,
                domain_name, l7_proto, 
                 sum(bsc) as bsc ,  
                sum(bcs) as bcs ,   sum(pcs) as pcs, sum(psc) as psc
                ,sum(hit) as hit 
                , app_id, l4_proto_id,port
                from log_stats_servertraffic1hour
                where  time >= (select time from times_Education1Day  order by time asc limit 1 )  AND time < (select time from times_Education1Day  order by time desc limit 1 ) + interval '1 day'
		and detection_history ~ '28099m'
                group by ip, domain_name, trunc, l7_proto, l4_proto_id, app_id, action_id,port
