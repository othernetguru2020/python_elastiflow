#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import logging
from elasticsearch import Elasticsearch
from elastic_queries import *
import json
import elastic_constants

es_server_port = elastic_constants.ES_PORT

parser = argparse.ArgumentParser(prog='elastic.py',
                                 description='Get log entries from elastic server.')
parser.add_argument("-t","--type", help="get specific type of logs (log,flow)")
parser.add_argument("-at","--aggtype", help="agg flows by AS or CA")
parser.add_argument("-time","--timewindow", help="timewindow m = minutes, h = hour, d = day"
                                                 " For example [now - x[m,h,d]] window variable can"
                                                 " be 15m,60m,5h,etc")

parser.add_argument("-s","--servername", help="The elasticsearch server name")
parser.add_argument("-l","--syslogsourceadd", help="syslog source")
parser.add_argument("-f","--flowsourceadd", help="flow source address")
parser.add_argument("-if","--fileflowsourceadd", help="flow source address list")
parser.add_argument("-w","--source", help="flow source area GENERAL = bdr-gcc ,SCIENCE = bdr-fcc,SBNFD,SBNND,CMS")

args = parser.parse_args()

print(args.source)

if args.source:
    source = source_area_text(args.source)

if args.timewindow:
    search_window_in_minutes = "now-" + str(args.timewindow)
else:
    search_window_in_minutes = elastic_constants.ES_SEARCH_TIME_WINDOW

if not args.servername:
    es_server = elastic_constants.ES_SERVER

if args.syslogsourceadd:
    user_request_log = args.syslogsourceadd
else:
    user_request_log = elastic_constants.SYSLOG_DEFAULT_SEARCH_PARAM

if args.flowsourceadd:
    user_request_flow = args.flowsourceadd
else:
    user_request_flow = elastic_constants.FLOW_DEFAULT_SOURCE_SEARCH_PARAM

if args.aggtype:
    if args.aggtype == "AS":
        agg_type = "flow.client_autonomous_system"
    elif args.aggtype == "CA":
        agg_type = "flow.client_addr"
    elif args.aggtype == "SP":
        agg_type = "flow.src_port"


if not args.type:
    type = "flow"
else:
    type = args.type

try:
    # concatenate a string for the Elasticsearch connection
    es_srv_string = es_server + ":" + es_server_port
    # declare a client instance of the Python Elasticsearch library
    client = Elasticsearch(es_srv_string)
    # info() method raises error if domain or conn is invalid
    #print(json.dumps(Elasticsearch.info(client), indent=4), "\n")

except Exception as err:
    print("Elasticsearch() ERROR:", err, "\n")
    # client is set to none if connection is invalid
    client = None

def pretty(node, indent=0):
    if type(node) is list:
        for item in node:
            print(item)
            return 0
    elif type(node) is dict:
        for key, item in node.items():
            print(key, "-->", item)
            if type(item) is dict:
                pretty(item)
        return 1
    else:
        print(node)
        return 2

def dict_walk(d,indent=1):
    #if d['_source']['source']:
    #    print(d['_source']['source'])
    #if d['_source']['message']:
    #    print(d['_source']['message'])
    for key, value in d.items():
        print('\t:' * indent + str(key))
        if isinstance(value, dict):
            #print(value)
            dict_walk(value, indent + 1)
        else:
        #    if key == "source" or key == "full_message":
            print('\t --' * (indent + 1) + str(value))


def find_latest_indicies(node):
    graylog_list = []
    elastiflow_list = []
    for key,item in node.items():
        if "elastiflow-" in key:
            elastiflow_list.append(key)
            continue
        elif "graylog" in key:
            graylog_list.append(key)
            continue
    graylog_list.sort()
    elastiflow_list.sort()
    #print(graylog_list[-1],elastiflow_list[-1])
    return graylog_list[-1],elastiflow_list[-1]

def list_hits(hit_dict):
    #all_hits = graylog_results['hits']['hits']
    all_hits = hit_dict['hits']['hits']
    # see how many "hits" it returned using the len() function
    print ("total hits using 'size' param:", len(all_hits))
    # iterate the nested dictionaries inside the ["hits"]["hits"] list
    #dict_walk(all_hits)
    #print(json.dumps(es_results,indent=4))
    for num, doc in enumerate(all_hits):
        #print ("DOC ID:", doc["_id"], "--->", doc, type(doc), "\n")
        print(doc)
        dict_walk(doc)

def search_elasticsearch_data(client,type, page=0):
    # User makes a request on client side
    #type can be flow or log
    index_raw = client.indices.get_alias()
    #print(index_raw)
    graylog_index,elastiflow_index = find_latest_indicies(index_raw)

    if client != None:
        if type =='log':
            query = log_query(user_request_log)
            result = client.search(index=graylog_index, body=query, size=10)

        elif type == 'flow':
            print(source)
            query = flow_query(user_request_flow,str(source),str(agg_type),search_window_in_minutes)
            result = client.search(index=elastiflow_index, body=query, size=0)
        else:
            return "Type Error"
    return result
#"field": "flow.client_addr"
#flow.client_autonomous_system
# Take the user's parameters and put them into a Python
# dictionary structured like an Elasticsearch query:

#print(query)

es_results = {}
es_results = search_elasticsearch_data(client, type)
#print(json.dumps(es_results,sort_keys=True, indent=4))
#list_hits(es_results)
#print("total hits:", len(es_results["hits"]["hits"]))
for k in es_results["aggregations"]["genres"]["buckets"]:
    print(k["key"],k["doc_count"])
print(es_results["aggregations"]["genres"]["buckets"])


#list_hits(sflow_results["hits"]["hits"])
#list_hits(graylog_results["hits"]["hits"])

