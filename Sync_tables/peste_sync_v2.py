import csv
from django.db import connection
from zafirmetadata.models import PesteMainIp, PesteIpActivity
from django.core.management.base import BaseCommand

class Command(BaseCommand):

    def handle(self, *args, **options):
        print("Starting to sync")
        peste_sync()
        print("Synchronization done")


def peste_sync():
    # load Data from file, headers should be ip, payloa, service, status, priority
    with open('/home/user/Fara/tetst_pestemainip.csv') as f:
        reader = csv.reader(f, delimiter=',', skipinitialspace=True)
        next(reader, None)  # skip headers line
        # structure of data : {(ip,payload):[service, status, priority]}
        File_data = {(line[0], int(line[1])): [int(line[2]), int(line[3]), int(line[4])] for line in reader}

    # fetch Data from Table
    cur = connection.cursor()
    cur.execute('select ip, payload, service, status, priority from zafirmetadata_PesteMainIp')
    out = cur.fetchall()
    Table_data =   {(ip,payload):[service, status, priority] for ip, payload, service, status, priority in out }

    # File_data - Table_data, insert part
    File_Table = {k : File_data[k] for k in set(File_data) - set(Table_data)}

    #Table_data - File_data, delete part
    Table_File = {k : Table_data[k] for k in  set(Table_data) - set(File_data)}

    # intersection part, update part
    intersection_key = File_data.keys() & Table_data.keys()

    # update part
    counter = 0
    for key in intersection_key:
        if File_Table[key] != Table_File[key]:
            counter += 1
            PesteMainIp.objects.filter(ip= key[0], payload=key[1]).update(service=File_Table[key][0], status=File_Table[key][1], priority=File_Table[key][2])
    print("number of updated row: ",counter)

    # insert part
    insert_obj = []
    BATCH_SIZE = 100
    for key in File_Table.keys():
        insert_obj.append(PesteMainIp(
            ip=key[0], payload=key[1],
            service= File_Table[key][0],
            status = File_Table[key][1],
            priority = File_Table[key][2]
        ))
    PesteMainIp.objects.bulk_create(insert_obj, batch_size=BATCH_SIZE)
    print("number of inseted row: ",len(insert_obj))

    # delete part
    counter  = 0
    for key in Table_File.keys():
        queryset = PesteMainIp.objects.filter(ip = key[0], payload = key[1],service = Table_File[key][0], status = Table_File[key][1], priority =Table_File[key][2])
        queryset._raw_delete(queryset.db)
        counter +=1
    print("number of deleted row: ",counter)