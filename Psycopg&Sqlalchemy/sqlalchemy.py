from sqlalchemy.sql import table, select
from sqlalchemy import text


def raw_sql_generator( table_name, key_columns, agg_columns,resolution):
    tab = table(table_name)
    query = select(tab,text(', '.join(map(str,[col for col in key_columns]))+','+\
        'date_trunc(\'{}\',time) as trunc'.format(resolution)+','+\
    ','.join(map(str,[col for col in agg_columns])))).\
        where(text('time >= {start} and time < {end}'.format(start ='start',end=  'end'))).\
    group_by(text('trunc, ' + ', '.join(map(str,[col for col in key_columns]))))

    return  print(query)

raw_sql_generator('test',['a','b'],['sum(c)','count(d)'],'day')
# output :
# SELECT a, b,date_trunc('day',time) as trunc,sum(c),count(d)
# FROM test
# WHERE time >= start and time < end GROUP BY trunc, a, b