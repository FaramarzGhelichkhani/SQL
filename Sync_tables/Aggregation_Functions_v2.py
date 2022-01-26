def Aggregations_Time_Finder(src_model, dest_model, resolution):
    """
    src_model:  name of suroce maodel which be aggregated.
    dest_model: name of destination model.
    resolution: time period of  aggregation. should be 'day' or 'hour'.
    return: list of time that are in source model table and not in destination model table.

    with this function we connect two times to databese to fetch distinct truncated time both for src and dest model
     and then calculte the difference(relative complement) between them.
    """
    query = Queries['distinct_time']
    from django.db import connection

    with connection.cursor() as cursor:
        cursor.execute(query.format(table=src_model._meta.db_table,resolution=resolution))
        src_model_times = cursor.fetchall()
        cursor.execute(query.format(table=dest_model._meta.db_table,resolution=resolution))
        dest_model_times = cursor.fetchall()
        cursor.close()

    agg_times = list(set(src_model_times).difference(set(dest_model_times)))
    agg_times.sort()
    return agg_times

def sync(query,columns,dest_model):
    """
    query:       query for aggregation, it selects from Queries dict in the sync_with_model function.
    columns :    coulmns of dest table. orders based on query.
    dest_model:  name of destination model

    with this function Django connect to the database to run query and fetch data  into in-memory csv file then
    again connect to the database to import data from the csv file to the dest table.
    """
    from django.db import connection
    from io import StringIO

    with connection.cursor() as cursor, StringIO() as mem_file:
        cursor.copy_expert(sql='copy ({}) to STDOUT csv'.format(query), file=mem_file)
        mem_file.seek(0)
        cursor.copy_expert(sql='copy {}({}) from STDIN csv'.format(dest_model._meta.db_table,','.join(columns)), file=mem_file)
        cursor.close()


def sync_with_model(src_model, dest_model, resolution, query,site_id=None):
    """
    this function first finds times for aggregation by using the Aggregations_Time_Finder function,
    then aggregate on the src table for every 1hour or 1day interval and then import result to dest table
    by using the sync function.
    """

    from dateutil.relativedelta import relativedelta

    Agg_Times = Aggregations_Time_Finder(src_model, dest_model, resolution)
    diffday  = {'day': 1 , 'hour':0}
    diffhour = {'day': 0 , 'hour':1}
    for time in  Agg_Times:
        start_time = time[0]
        end_time   = time[0] + relativedelta(days=diffday[resolution],hours=diffhour[resolution])
        print("time: ", time[0])
        Query = Queries[query][0]
        columns = Queries[query][1]
        Query = Query.format(src=src_model._meta.db_table,start=start_time, end=end_time,resolution=resolution,site_id = site_id)
        sync(Query,columns,dest_model)


Queries = {
'distinct_time' : """ WITH RECURSIVE log AS (
       (SELECT date_trunc('{resolution}',time) AS  time FROM {table} ORDER BY time DESC LIMIT 1)
       UNION ALL
       SELECT (
          SELECT date_trunc('{resolution}',time) FROM {table}
          WHERE time < t.time
          ORDER BY time DESC LIMIT 1
       )
       FROM log t
      WHERE t.time IS NOT NULL
    )
    SELECT time FROM log 
    where time is not null and time  <  date_trunc('{resolution}',now())
    order by time
    """,
'ip_domain': [""" select site_id, ip ,
                date_trunc('{resolution}',time) as trunc ,
                domain_name,(select  substring(domain_name
                from  '[A-Za-z0-9\-]+\.[a-z]+$')) as secondleveldomain,l7_proto, sum(hit) as hit , sum(bsc) as bsc ,  
                sum(bcs) as bcs ,  sum(psc) as psc, sum(pcs) as pcs
                from {src}
                where  time >= '{start}' AND time < '{end}' and site_id = {site_id}
                and domain_name !=''
                group by site_id, ip, domain_name, trunc, l7_proto""",
              ['site_id','ip','time','domain_name','secondleveldomain','l7_proto','hit','bsc','bcs','psc','pcs']],
'zaffiraggstats':[ """
     with log as (select site_id , ip , unnest(agg) as agg,trunc, total_bsc,total_bcs,total_psc,total_pcs,
      totalipport_byte , total_hit , totalipport_hit from(
     select site_id , ip , array_agg(distinct app_id::varchar || ','|| action_id::varchar) as agg ,
     date_trunc('{resolution}',time) as trunc, 0 as total_bsc , 0 as total_bcs , 0 as total_psc,0 as total_pcs , 
     sum(bsc+bcs) as totalipport_byte , 0 as total_hit, sum(hit) as totalipport_hit 
     from {src}
      where  site_id={site_id} AND time >= '{start}' AND time < '{end}'
      group by site_id,ip , trunc ) as unser),

      log2 as (select site_id , ip , app_id::varchar || ','|| action_id::varchar as agg , 
      date_trunc('{resolution}',time) as trunc , sum(bsc) as total_bsc,sum(bcs) as total_bcs , sum(psc) as total_psc
       , sum(pcs) as total_pcs , 0 as totalipport_byte , sum(hit) as total_hit ,0 as totalipport_hit
      from {src}
      where  site_id={site_id} AND time >= '{start}' AND time < '{end}'
      group by site_id , ip , agg, trunc),
      log3 as(select site_id , ip , agg ,trunc ,sum(total_bsc) as total_bsc ,sum(total_bcs) as total_bcs ,
       sum(total_psc) as total_psc,sum(total_pcs) as total_pcs  , sum(totalipport_byte) as totalipport_byte , 
       sum(total_hit) as total_hit , sum(totalipport_hit) as totalipport_hit
      from (select * from log union all select * from log2 ) as logging
      group by site_id , ip ,agg, trunc)

      select site_id,ip, trunc,split_part(agg, ',', 1)::integer as app_id , total_bsc, total_bcs,total_pcs,total_psc
      , (total_bcs+total_bsc)*100/totalipport_byte , total_hit , 
      total_hit*100/totalipport_hit,split_part(agg, ',', 2)::integer  as action_id
      from log3 """,
    [
    'site_id','ip','time','app_id','bsc','bcs','pcs','psc','tbpercent','hit','thpercent','action_id'
    ]],
'ip_app_domain' : [""" select  action_id, ip ,
                date_trunc('{resolution}',time) as trunc ,
                domain_name, l7_proto, 
                 sum(bsc) as bsc ,  
                sum(bcs) as bcs ,   sum(pcs) as pcs, sum(psc) as psc
                ,sum(hit) as hit 
                , app_id, l4_proto_id
                from {src}
                where  time >= '{start}' AND time < '{end}'
                group by ip, domain_name, trunc, l7_proto, l4_proto_id, app_id, action_id """,
    ['action_id','ip','time','domain_name','l7_proto','bsc','bcs','pcs','psc','hit','app_id','l4_proto_id']]
}