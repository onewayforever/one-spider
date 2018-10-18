import pymongo
import requests
import json
import pandas as pd
from spider_db.db import SpiderDB,Table

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

class TongjijuDB(SpiderDB):

    def display_docs_readable(self,deep=-1):
        if deep>=0:
            data = self.coll_meta.find({'trace':{'$size':deep}})
        else:
            data = self.coll_meta.find()
        l = list(map(lambda x:(x['query_type'],x['title'],x['trace_name'],x['id_name'],x['trace'],x['id']),data))
        for item in l:
            print(item)


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
