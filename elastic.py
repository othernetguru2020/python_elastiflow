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
    es_srv_string = es_server + ":" + es_server_port
    client = Elasticsearch(es_srv_string)

except Exception as err:
    print("Elasticsearch() ERROR:", err, "\n")
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
    for key, value in d.items():
        print('\t:' * indent + str(key))
        if isinstance(value, dict):
            dict_walk(value, indent + 1)
        else:
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
    return graylog_list[-1],elastiflow_list[-1]

def list_hits(hit_dict):
    all_hits = hit_dict['hits']['hits']
    print ("total hits using 'size' param:", len(all_hits))
    for num, doc in enumerate(all_hits):
        print(doc)
        dict_walk(doc)

def search_elasticsearch_data(client,type, page=0):
    index_raw = client.indices.get_alias()
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

es_results = {}
es_results = search_elasticsearch_data(client, type)
for k in es_results["aggregations"]["genres"]["buckets"]:
    print(k["key"],k["doc_count"])
print(es_results["aggregations"]["genres"]["buckets"])
