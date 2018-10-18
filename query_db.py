#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import pymongo
import requests
import json
import pandas as pd
from spider_db.client import SpiderDBClient 
#from spider_db.tongjiju import TongjijuDB 



if __name__ == '__main__':
    spider_client = SpiderDBClient(host='10.0.50.141',user='wanw',passwd='111111')
    db=spider_client.use_db('tongjiju')
    db.display_docs_readable(1)
    print(db.docs_count())
    db.docs_readable(db.find_table_by_keyword('教育'))
    table = db.get_table(trace=['zb', 'A0M'],id='A0M0B')
    print(table)
    options = table.get_options()
    print(options)
    option_v = []
    for k in options.keys():
        option_v.append((k,options[k]['options'][0][0]))
    data = table.fetch_data(option_v) 
    print(data)
    print(table.to_readable())
    exit(0)
