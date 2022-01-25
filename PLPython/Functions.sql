DROP FUNCTION if exists inet_exclude_py(text,text[]);
CREATE OR REPLACE FUNCTION public.inet_exclude_py(
	net1 text, nets text[])
    RETURNS inet[]
    LANGUAGE 'plpython3u'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
import ipaddress;
from ipaddress import ip_network;
from netaddr import IPSet;
main = IPSet([net1]);
nets_set = IPSet(nets);
res = main.difference(nets_set);
result = res.iter_cidrs()
return result;
END;
$BODY$;
--------------------------------------------------------
DROP FUNCTION IF EXISTS public.inet_merge_py(inet[]);
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
-----------------------------------------------------------
-- FUNCTION: public.asn_voting2(jsonb, jsonb)-- DROP FUNCTION IF EXISTS public.asn_voting2(jsonb, jsonb);CREATE OR REPLACE FUNCTION public.asn_voting2(
	voting_dict jsonb,
	asn_dict jsonb)
    RETURNS text
    LANGUAGE 'plpython3u'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	import json
	
	try:
		vot_dict = json.loads(voting_dict)
		asndict =  json.loads(asn_dict)		asncount = {}
		for dict in asndict:
			asncount.update({dict["asn"]:dict["count_dbname"]})		asn_votes= {}
		weghit = {'true_equals':3,'true_contains':2,'true_is contained within':1,'false_equals':-3,'false_contains':-2,'false_is contained within':-1}
		for dict in vot_dict:
			value = 0
			for k,v in dict["votes"].items():
				value += weghit[k]*v
			asn_votes.update({dict["asn"]:abs(value)})					
                asn_final_votes  = {k: asncount.get(k, 0) + asn_votes.get(k, 0) for k in set(asncount) & set(asn_votes)}
		nominate_asn = str(max(asn_final_votes, key=asn_final_votes.get))
		if nominate_asn =='-' and len(asndict) > 1:
			del asn_final_votes['-']
			return str(max(asn_final_votes, key=asn_final_votes.get))
		else:
			return nominate_asn
	except:
		pass
	
$BODY$;ALTER FUNCTION public.asn_voting2(jsonb, jsonb)
    OWNER TO zafir;
