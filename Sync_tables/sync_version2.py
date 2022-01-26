import csv
from django.db import connection
from django.core.management.base import BaseCommand
from django.apps import apps

class Command(BaseCommand):

    def add_arguments(self, parser):
        parser.add_argument('model', type= str, help="Name of django model" )
        parser.add_argument('path', type=str, help="Path of csv file")

    def handle(self, *args, **kwargs):
        model_name = kwargs['model']
        app = 'zafirmetadata'
        model = apps.get_model(app,model_name)
        path = kwargs['path']

        peste_sync(model,path)

def peste_sync(model,path):
    """
    param1 model: name of model that will be sync.
    param2 path:  path of csv file.
    this function first create a temporary table same as model then copy csv file into to the temporary table,
    then in three part first delete from model every row that doesn't exists in temporary table, second update
    every row that have same indexs field in temporary table and finally insert every row that exist in temporary table
    and   doesn't exist in model.
    """
    with open(path) as csv_data:
        csv_reader = csv.reader(csv_data)
        header = next(csv_reader)
    with connection.cursor() as cur:
        sql_query = """create temp table file 
                       as 
                       select * from {table} limit 0;
                      COPY file({columns}) FROM STDIN With CSV HEADER DELIMITER  ',';"""
        cur.copy_expert(sql=sql_query.format(table = model._meta.db_table,columns= ','.join(header)), file=csv_data)

        # indexes fields are unique in table or csv file.
        key_fields = model._meta.indexes[0].fields
        other_fields = list(set(header) - set(key_fields))

       # delete part
        delete_query_raw  = """ delete from {table} t
                            where not exists(select 3 from file f where f.{key1}= t.{key1} and f.{key2} = t.{key2} )"""
        delete_query = delete_query_raw.format(table= model._meta.db_table, key1=key_fields[0], key2=key_fields[1])
        cur.execute(delete_query)
        print(cur.statusmessage)

        #update part
        update_query_raw = """ UPDATE {table} t
                            SET   ({field1}, {field2}, {field3}) = (file.{field1}, file.{field2}, file.{field3})
                            FROM file
                            where 
                            file.{key1}= t.{key1} and file.{key2} = t.{key2}
                            ;"""
        update_query = update_query_raw.format(table= model._meta.db_table, key1=key_fields[0], key2=key_fields[1],field1= other_fields[0],field2=other_fields[1],field3=other_fields[2])
        cur.execute(update_query)
        print(cur.statusmessage)

        # insert part
        insert_query_raw = """ insert into {table} ({all_columns})
                            select
                            {all_columns}
                            from  file f
                            where not exists (select 3 from {table} t where f.{key1} = t.{key1} and f.{key2} = t.{key2} );"""
        insert_query = insert_query_raw.format(table=model._meta.db_table,all_columns = ','.join(header), key1=key_fields[0], key2=key_fields[1])
        cur.execute(insert_query)
        print(cur.statusmessage)
