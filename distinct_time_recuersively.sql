WITH RECURSIVE log AS (
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