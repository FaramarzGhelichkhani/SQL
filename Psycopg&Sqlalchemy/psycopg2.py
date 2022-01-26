#!/usr/bin/env python
# coding: utf-8


import psycopg2




conn = psycopg2.connect(host="192.168.107.30", port = 5432, database="ip2location_lite", user="postgres", password="-")

cur = conn.cursor()


cur.execute(""" with agg as (SELECT array_agg(cidr::inet) as arr, as_name,asn  FROM ip2location_asn 
group by as_name, asn),
dist as (select DISTINCT as_name , asn from ip2location_asn)
select  agg.arr , agg.as_name , dist.asn 
from agg
join dist
on agg.as_name = dist.as_name and  agg.asn = dist.asn

""")



result = cur.fetchall()

result_list =[[r[0],r[1],r[2]] for r in result]


result_list[112][0]

from netaddr import  cidr_merge

merged_list = result_list

for i in range(len(merged_list)):
    merged_list[i][0] = cidr_merge(merged_list[i][0])



postgres_insert_query = """ INSERT INTO ip2location_asn_merged (net, name, asn) VALUES (%s::inet,%s,%s)"""


for data in merged_list: 
    for net in data[0]:
        record_to_insert = (str(net), data[1], data[2])
        cur.execute(postgres_insert_query, record_to_insert)


conn.commit()

cur.close()
conn.close()

