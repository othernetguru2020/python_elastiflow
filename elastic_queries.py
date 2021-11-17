def source_area_text(source):
    if source == "GENERAL":
        area_flow_source_ip = "10.10.10.0"
    return area_flow_source_ip

def flow_query(user_request_flow,source,agg_type,timewindow):
    flow_query_command = {
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "range": {
                                        "@timestamp": {
                                            "gte": timewindow,
                                            "lte": "now"
                                        }
                                    }
                                },
                                {
                                    "match": {
                                        "flow.server_addr": user_request_flow
                                    }
                                },
                                {
                                    "match": {
                                        "node.hostname": source
                                    }
                                }
                            ]
                        }
                    },
                    "aggs": {
                        "genres": {
                            "terms": {
                                "field": agg_type ,
                                "order": {"_count": "desc"}
                            }
                        }
                    }
    }
    return flow_query_command

def log_query(user_request_log):
    log_query_command = {
                    "query": {
                        "wildcard": {
                            "gl2_remote_ip": {"wildcard": user_request_log}
                        }
                    },
                    "sort": [{"timestamp": {"order": "desc"}}]
                }
    return log_query_command