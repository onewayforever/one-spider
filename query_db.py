#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import pymongo
import requests
import json
import pandas as pd


    

class Table(object):
    def __init__(self,meta):
        self.meta = meta 
        self.option_dict = None
        self.data = None

    def get_options(self):
        pass

    def fetch_data(self):
        pass


class SpiderDB(object):
    def __init__(self,db_name,host='127.0.0.1', port=27017):
        self.client = pymongo.MongoClient(host=host, port=port)
        db_info = My_DBS.get(db_name)
        assert db_info != None
        self.name = db_name
        self.db_info = db_info
        self.spider_db = self.client[db_info['db_name']]
        #self.spider_db = client['tongjiju_db']
        self.coll_meta = self.spider_db[db_info['meta']]
        self.coll_data= self.spider_db[db_info['data']]
        self.Table =  db_info['table_class']

    def all_docs(self):
        return self.coll_meta.find()
    
    def all_docs_readable(self,deep=-1):
        if deep>=0:
            data = self.coll_meta.find({'trace':{'$size':deep}})
        else:
            data = self.coll_meta.find()
        l = list(map(lambda x:(x['query_type'],x['title'],x['trace_name'],x['id_name'],x['trace'],x['id']),data))
        for item in l:
            print(item)

    def docs_count(self):
        return self.coll_meta.find().count()
    
    def get_table(self,id):
        data = self.coll_meta.find_one({'id':id})
        return self.Table(data)

def wdnodes2dict(nodes):
    wd_dict = {}
    for node in nodes:
        key = node.get('wdcode')
        option_dict = {}
        for n in node.get('nodes'):
            code = n.get('code')
            option_dict[code] = {
                'cname':n.get('cname'),
                'unit':n.get('unit'),
                'name':n.get('name'),
                'exp':n.get('exp')
            }
        wd_dict[key] = {
            'name':node.get('wdname'),
            'dict':option_dict
        }
    return wd_dict


class TongjijuTable(Table):
    def get_options(self):
        if self.option_dict:
            return self.option_dict
        options = self.meta.get('options')
        option_dict = {}
        for item in options:
            key = item['wdcode']
            key_name = item['wdname']
            droplist = list(map(lambda x: (x['code'],x['name']),item['nodes'])) 
            option_dict[key] = {"name":key_name,"options":droplist}
        self.option_dict = option_dict 
        return option_dict

    def fetch_data(self,options):
        query_url = {
            "easyquery":'http://data.stats.gov.cn/easyquery.htm?m=%s',
            "tablequery":'http://data.stats.gov.cn/tablequery.htm?m=%s'
        }
        url = query_url[self.meta['query_type']]%'QueryData' 
        params = {}
        if self.meta['query_type']=='easyquery':
            wds = []
            dfwds = []
            for item in options:
                dfwds.append({'wdcode':item[0],'valuecode':item[1]})
            params = {
                    'dbcode':self.meta['dbcode'],
                    'rowcode':self.meta['rowcode'],
                    'wds':json.dumps(wds),
                    'dfwds':json.dumps(dfwds)
                }
        if self.meta['query_type']=='tablequery':
            wds = []
            #dfwds = []
            for item in options:
                wds.append({'wdcode':item[0],'valuecode':item[1]})
            params = {
                    'code':self.meta['id'],
                    'wds':json.dumps(wds),
                    #'dfwds':json.dumps(dfwds)
                }
        response = requests.get(url,params=params)
        data = response.json() 
        #self.data = data#data['returndata']
        if data.get('returndata'):
            self.data = data.get('returndata')
        else:
            self.data = data
        return self.data
    def to_readable(self):
        assert self.data is not None
        data = self.data
        wdnodes = data.get('wdnodes')
        if wdnodes is None:
            return data    
        self.wd_dict = wdnodes2dict(wdnodes)
        #print(self.wd_dict)
        datanodes = data.get('datanodes') 
        readable = []
        for item in datanodes:
            if item['data']['hasdata'] == False:
                continue
            #print(item.get('wds'))
            wds = list(map(lambda x: self.wd_dict[x['wdcode']]['dict'][x['valuecode']]['name'],item.get('wds')))
            readable.append((wds,item['data']['data'])) 
        return readable
        items = list(map(lambda x:x['data'],datanodes))
        return pd.DataFrame(datanodes)

My_DBS = {
    'tongjiju':{
        'db_name':'tongjiju_db',
        'meta':'tree_meta',
        'data':'tree_data',
        'table_class':TongjijuTable
    }
}


if __name__ == '__main__':
    db = SpiderDB('tongjiju')
    db.all_docs_readable(1)
    print(db.docs_count())
    table = db.get_table('A02')
    print(table)
    options = table.get_options()
    print(options)
    data = table.fetch_data([('reg','141')]) 
    print(data)
    print(table.to_readable())
