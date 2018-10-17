#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2018-10-12 22:50:03
# Project: data_stats_gov_cn_tree

from pyspider.libs.base_handler import *
import json
import re
import pymongo
import copy
import datetime
from pyspider.libs.utils import md5string
import time

meta_collection = 'tree_meta'

db_p = re.compile(r"drawChart.DbCode\(\"(.+?)\"\)")
row_p = re.compile(r"drawChart.RowCode\(\"(.+?)\"\)")
col_p = re.compile(r"drawChart.ColCode\(\"(.+?)\"\)")
roots_p = re.compile(r"var rootTree = \'(.+?)\';")
href_easy_p = re.compile(r".*/easyquery.htm\?cn=(.*)")
href_table_p = re.compile(r".*/tablequery.htm\?code=(.*)")

query_url = {
    "easyquery":'http://data.stats.gov.cn/easyquery.htm?m=%s',
    "tablequery":'http://data.stats.gov.cn/tablequery.htm?m=%s'
}
#easyquery_Url='http://data.stats.gov.cn/easyquery.htm?m=%s'
#tablequery_Url='http://data.stats.gov.cn/tablequery.htm?m=%s'

client = pymongo.MongoClient(host='127.0.0.1', port=27017)
spider_db = client['tongjiju_db']

class Handler(BaseHandler):
    crawl_config = {
    }

    #@every(minutes=3)
    def on_start(self):
        print('I am comming')
        self.crawl('http://data.stats.gov.cn/', callback=self.index_page)

    #@config(age=3 )
    def index_page(self, response):
        all_href = []
        #遍历所有easyquery表格和tablequery表格
        for each in response.doc('a[href^="http"]').items():
            href = each.attr.href;
            params = {}
            easy_urls = href_easy_p.findall(href)
            if len(easy_urls)>0:    
                all_href.append({'label':each.text(),'url':href})
                section = easy_urls[0]
                t = time.time()
                timestamp = int(round(t * 1000))
                params['k1']=timestamp
                self.crawl(href, callback=self.detail_page,params = params,save={'url':href,'section':section,'title':each.text(),'trace':[],'query_type':'easyquery'})
            
            table_urls = href_table_p.findall(href)
            if len(table_urls)>0:    
                all_href.append({'label':each.text(),'url':href})
                section = table_urls[0]
                t = time.time()
                timestamp = int(round(t * 1000))
                params['k1']=timestamp
                self.crawl(href, callback=self.detail_page,params = params,save={'url':href,'section':section,'title':each.text(),'trace':[],'query_type':'tablequery'})
        return {
            "urls":all_href
        }

    #@config(priority=2)
    def detail_page(self, response):
        
        '''
        解析这段代码，找到访问的指标的数据库，指标的树形结构等
        <script type="text/javascript">
      var drawChart = new cubetable();
      drawChart.DbCode("fsjd");
      drawChart.RowCode("reg");
      drawChart.ColCode("sj");
      var ifautodelblanksj=true;
      var zbpath=["zb"];
      var zbcode="";
      var rootTree = '[{"dbcode":"fsjd","id":"zb","isParent":true,"name":"指标","pid":"0","wdcode":"zb"},{"dbcode":"fsjd","id":"reg","isParent":true,"name":"地区","pid":"0","wdcode":"reg"}]';
      if (rootTree != "" && rootTree != "[]"){
        rootTree = eval('(' + rootTree + ')');
      }
      else {
        rootTree = [];
      }
dtList
        '''
        text = response.text;
        #dtList =  response.doc('body > .dtList').text();
        script =  response.doc('body > script').text();
        #begin = script.find('var drawChart = new cubetable()')
        #end = script.find('if (rootTree != "" && rootTree != "[]"){')
        #script = script[begin:end]
        #db = db_p.findall(script)[0]
        #row = row_p.findall(script)
        #col = col_p.findall(script)
        roots = [] if len(roots_p.findall(script))==0 else json.loads(roots_p.findall(script)[0])
        
        #section_info = {
        #    'db':db,
        #    'rowcode':row,
        #    'colcode':col
        #}
        
        #解析指标树
        url = query_url[response.save['query_type']]
        for root in roots:
            tree_url = url%'getTree'
            info = copy.deepcopy(response.save)
            info['trace'].append(root)
            t = time.time()
            timestamp = int(round(t * 1000))
            root['k1']=timestamp
            self.crawl(tree_url, callback=self.detail_tree,params=root,save=info)#section_info)
            info = None
        return {
            "info":json.dumps(info),
            #"url": response.url,
            #"db": db,
            #"script":script,
            #"row":row,
            #"col":col,
            "roots":roots,
            #"text":text
        }        
    
    
    def detail_tree(self,response):
        '''
        树的格式为
        [{'dbcode': 'fsjd',
           'id': 'A01',
           'isParent': True,
           'name': '国民经济核算',
           'pid': '',
           'wdcode': 'zb'},
          {'dbcode': 'fsjd',
           'id': 'A02',
           'isParent': True,
           'name': '建筑业',
           'pid': '',
           'wdcode': 'zb'},
          {'dbcode': 'fsjd',
           'id': 'A05',
           'isParent': True,
           'name': '价格指数',
           'pid': '',
           'wdcode': 'zb'}]
        '''
        tree = json.loads(response.content.decode('utf-8'))#['returndata'] 
        #tree = response.json;
        
        for node in tree:
            if node.get('isParent')==True:
                info = copy.deepcopy(response.save);
                info['trace'].append(node)
                url =  query_url[info['query_type']]%'getTree'
                self.crawl(url, callback=self.detail_tree,params=node,save=info)
            else:
                #self.fetch_node_data(node,info)
                #info = response.save
                info = copy.deepcopy(response.save);
                info['node'] = node
                info['node_url'] = response.url
                #trace = list(map(lambda x:x['id'],info['trace']))
                #trace_name = list(map(lambda x:x['name'],info['trace']))
                #key = node['id']
                #key_name = node['name']
                #coll = spider_db['tree_meta']
                #data = result.get('data')
                self.fetch_node_wds(info)
                #data_id = coll.update({'title':info['title'],'trace':trace,'key':key,'section':info['query_type']},{'title':info['title'],'trace':trace,'key':key,'section':info['query_type'],'trace_name':trace_name,'key_name':key_name,'node':node}, True )
            #info = None;
            #self.save_node_data(node,info)
                #pass
                #url = 'http://data.stats.gov.cn/easyquery.htm?m=QueryData'                
                #self.crawl(url, callback=self.detail_node,params={'dbcode':node['dbcode'],'rowcode':node['wdcode'],'colcode':'sj','dfwds':[{'wdcode':node['wdcode'],'valuecode':node['id']}]})
            '''Node的格式
                {'dbcode': 'fsjd',
                  'id': 'A0501',
                  'isParent': False,
                  'name': '农产品生产价格指数',
                  'pid': 'A05',
                  'wdcode': 'zb'}
            '''
                
                
                #return { 'data':node}
        return {
            'url':response.url,
            'data':tree#response.json
        }

    def fetch_node_wds(self,meta):
        #先获得可选参数，再利用这些参数查询数据
        '''
                得到所有可选参数
                m: getOtherWds
                dbcode: fsyd
                rowcode: zb
                colcode: sj
                wds: []
                k1: 1539247352245
        '''
        #使用节点中的wdcode维度，代表表格的行，再查询以wdcode为行维度的话，还有哪些可选维度
        node = meta.get('node')
        query_type = meta['query_type']
        if query_type == 'easyquery':
            url = 'http://data.stats.gov.cn/easyquery.htm?m=getOtherWds' #query_url[query_type] % 'getOtherWds'
            params = {'dbcode':node.get('dbcode'),'rowcode':node.get('wdcode'),'wds':json.dumps([])}
            
        if query_type == 'tablequery':
            url = 'http://data.stats.gov.cn/tablequery.htm?m=OtherWds' #query_url[query_type] % 'OtherWds'    
            params = {'code':node.get('id')}
        
        t = time.time()
        timestamp = int(round(t * 1000))
        params['k1']=timestamp

        self.crawl(url, callback=self.detail_params,params=params,save=meta)
        #self.crawl(url, callback=self.detail_params,params={'dbcode':node['dbcode'],'rowcode':node['wdcode'],'colcode':info['colcode'],'wds':'[]'},save={'node':node,'section':info})

 ##
    #     获取数据表查询选项
    ##
    def detail_params(self,response):
        #query_type = response.save['query_type'];
        info = response.save;
        query_type = info['query_type']
        node = info['node'];
        #处理获得的维度信息
        '''
        [{'issj': False,
                          'nodes': [{'code': '110000',
                                     'name': '北京市',
                                     'sort': '1'},
                                    {'code': '650000',
                                     'name': '新疆维吾尔自治区',
                                     'sort': '1'}],
                          'selcode': '',
                          'wdcode': 'reg',
                          'wdname': '地区'},
                         {'issj': True,
                          'nodes': [{'code': 'LAST6',
                                     'name': '最近6季度',
                                     'sort': '4'},
                                    {'code': 'LAST12',
                                     'name': '最近12季度',
                                     'sort': '4'},
                                    {'code': 'LAST18',
                                     'name': '最近18季度',
                                     'sort': '4'}],
                          'selcode': 'last6',
                          'wdcode': 'sj',
                          'wdname': '时间'}]
        '''
        #dimensions = response.json['returndata']
         
        text = json.loads(response.content.decode('utf-8'))
        if type(text)==list:
            dimensions = text;
        if type(text)==dict:
            dimensions = text.get('returndata')

        trace = list(map(lambda x:x['id'],info['trace']))
        trace_name = list(map(lambda x:x['name'],info['trace']))
        id = node['id']
        id_name = node['name']
        coll = spider_db[meta_collection]
        dbcode = node.get('dbcode')
        pid = node['pid']
        rowcode = node.get('wdcode')
        obj = {'section':info['section'],'title':info['title'],'trace':trace,'id':id,'query_type':info['query_type'],'trace_name':trace_name,'id_name':id_name,'dbcode':dbcode,'pid':pid,'rowcode':rowcode,'node':node,'options':dimensions,'node_url':info['node_url'],'wds_url':response.url,'date':str(datetime.date.today())}
        coll.update({'title':info['title'],'trace':trace,'id':id,'query_type':info['query_type']},obj, True )
        return {
            'url':response.url,
            #'poll_codes':poll_codes,
            #'selcode':selcode,
            'obj':obj
        }

        poll_codes = []
        poll_label = None
        selcode = ''
        for d in dimensions:
            if False:#d['issj'] and ( d['nodes']['code'].startswith('LAST') or d['nodes']['code'].startswith('last')): #这个维度是时间，我们就采用覆盖面最大的那个
                last = 0;
                for n in d['nodes']:
                    #如果格式为LAST+number，先不考虑更复杂的情况
                    t_window = int(n['code'][4:])
                    if t_window>last:
                        last = t_window
                        selcode='last%d'%last
                        
            else: #这个维度很可能是，比如说区域
                poll_codes = d['nodes']
                poll_label = d['wdcode']
                
        #利用获得的参数去查询数据
        url = query_url[query_type] % 'QueryData'
        
        #url = Query_Url % 'QueryData'
        
        '''
        #可以按照子维度查询，比如区域
        m: QueryData
        dbcode: fsjd
        rowcode: zb
        colcode: sj
        wds: [{"wdcode":"reg","valuecode":"140000"}]
        dfwds: [{"wdcode":"zb","valuecode":"A0501"}]
        k1: 1539245279856
        '''
        '''
        for i in range(len(poll_codes)):
            code = poll_codes[i]
            params = {
                        'dbcode':node['dbcode'],
                       # 'rowcode':node['wdcode'],
                       # 'colcode':selcode,
                        'wds':json.dumps([{"wdcode":poll_label,"valuecode":code['code']}]),
                        'dfwds':json.dumps([{"wdcode":node['wdcode'],"valuecode":node['id']}])
                     
                     }
            new_save = copy.deepcopy(response.save)
            new_save['meta']['sub_id']=i
            self.crawl(url, callback=self.detail_node,params=params,save=new_save)
        '''     
                
        '''
        #没有按照子维度查询
        m: QueryData
        dbcode: hgyd
        rowcode: zb
        colcode: sj
        wds: []
        dfwds: [{"wdcode":"zb","valuecode":"A130501"}]
        k1: 1539309931840
        '''
        if len(poll_codes)==0:
            params = {
                        'dbcode':node['dbcode'],
                        'rowcode':node['wdcode'],
                        #'colcode':selcode,
                        'wds':[],
                        'dfwds':[{"wdcode":node['wdcode'],"valuecode":node['id']}]
                     
                     }
            self.crawl(url, callback=self.detail_node,params=params,save=response.save)
                            
        return {
            'url':response.url,
            'poll_codes':poll_codes,
            'selcode':selcode,
            'json':json.loads(response.content.decode('utf-8'))['returndata']
        }
           

    def get_taskid(self, task): 
        return md5string(task['url']+json.dumps(task['fetch'].get('data', ''))+str(datetime.date.today())+'v7.0')        
    

