# -*- coding: utf-8-sig -*-
"""
File Name ：    inv_num_get
Author :        Eric
Create date ：  2021/2/2

整理运营表中有效SKU，转换mapping后向BOP发起查询请求
"""
import pandas as pd
import re
import os
import requests
import json
from fuzzywuzzy import fuzz
from setting import Setting
import time


def time_decorator(func):
    def call_func(*args):
        print('开始运行任务%s'%func.__name__,time.ctime())
        t1 = time.time()
        func(*args)
        t2 = time.time()
        print('结束时间', time.ctime())
        print('%s 运行时间为 %d 分 %d 秒'%(func.__name__,(t2 - t1)//60, (t2-t1)%60))
    return call_func

class InvNumQuery(Setting):
    def __init__(self):
        super().__init__()
        self.file_path = self.file_settings['file_path']
        self.task_list = {
            'Todd':{
                    'file' : ['2019 Merchandising Todd-Yantai team handover from Jason.xlsx',
                                '1.1 Simplify Todd _Yantai team merchandising.xlsx',],
                    'sheet_pattern' : re.compile(r'\d+\-'),
                    },

            'Yafu':{
                    'file' : ['Yafu merchandising.xlsx',],
                    'sheet_pattern' : re.compile(r'^inventory$|^Grow bag combo$'),
                    },
            }
        self.combo_file = self.file_settings['combo_inv']

    def find_file(self,file):
        '''根据文件名选择原始文件夹下文件名最相似且匹配度大于75%的文件返回'''
        file = file.split('\\')[-1].replace('//','/').split('/')[-1]
        path = self.file_path
        file_list = []
        for r, _, f in os.walk(path):
            file_list.extend(f)
        choice, value = 0, ''
        for f in file_list:
            c = fuzz.ratio(f, file)
            if c > choice:
                choice, value = c, f
        if choice > 75:
            return value
        else:
            raise FileNotFoundError('Combo关系表未找到')

    def sku_stack(self,file, sheet_pattern):
        '''将指定文件有效sheet中的SKU进行合并'''
        if not os.path.exists(file): raise FileNotFoundError(f'{file}不存在')
        excel_file = pd.ExcelFile(file)
        result = pd.DataFrame()
        sheet_list = [sheet for sheet in excel_file.sheet_names if re.search(sheet_pattern, sheet)]
        for sheet in sheet_list:
            # 修改列名，并删除空格等无效字符
            iter_frame = pd.read_excel(file, sheet_name=sheet)
            iter_frame.columns = iter_frame.iloc[0].to_list()
            iter_frame.drop(0, inplace=True)
            iter_frame.columns = [str(column).strip().replace(' ', '') for column in iter_frame.columns.tolist()]
            # 目标列
            iter_frame = iter_frame[['ProductLine', 'ASIN', 'SKU']]
            iter_frame = iter_frame[iter_frame['ASIN'].apply(lambda x: True if re.search('^B+', str(x)) else False)]
            iter_frame['ProductLine'].fillna(method='ffill', inplace=True)
            result = pd.concat([result, iter_frame], ignore_index=True)
        # 删除多余空格
        result['SKU'] = result['SKU'].apply(lambda x: str(x).strip())
        # 去掉SKU为空的行
        result = result[~result['SKU'].isnull()]
        return result

    def sku_a_replace(self,sku_frame,):
        '''将-A产品的SKU替换为可查库存产品'''
        combo_file = os.path.join(self.file_path,self.find_file(self.combo_file))
        sheet_list = pd.ExcelFile(combo_file).sheet_names
        if not 'SKU' in sku_frame.columns.tolist():
            raise KeyError('sku_frame中不存在SKU列')
        if not '单品-A对应关系' in sheet_list or not 'combo-A对应关系' in sheet_list:
            raise KeyError('请检查文件对应-A sheet名')
        single_frame = pd.read_excel(combo_file, sheet_name='单品-A对应关系')
        combo_frame = pd.read_excel(combo_file, sheet_name='combo-A对应关系')
        combo_frame = pd.concat([single_frame, combo_frame], ignore_index=True)
        combo_frame.drop_duplicates(inplace=True)
        for column in combo_frame.columns:
            combo_frame[column] = combo_frame[column].apply(lambda x: str(x).strip())
        combo_frame.columns = ['SKU', 'SKU_rep', 'Num']
        # 将-A SKU替换为mapping
        sku_frame = sku_frame.merge(combo_frame, on='SKU', how='left')
        sku_frame.loc[~sku_frame['SKU_rep'].isnull(), 'SKU'] = sku_frame.loc[~sku_frame['SKU_rep'].isnull(), 'SKU_rep']
        return sku_frame

    def get_sku_list(self):
        '''将所有文件中SKU合并后以列表形式返回'''
        sku_frame = pd.DataFrame()
        task_list = self.task_list
        for task in task_list:
            task_file_list = task_list[task]['file']
            sheet_pattern = task_list[task]['sheet_pattern']
            for task_file in task_file_list:
                if not os.path.exists(os.path.join(self.file_path, task_file)):
                    task_file = self.find_file(task_file,)
                task_file = os.path.join(self.file_path, task_file)
                iter_frame = self.sku_stack(task_file, sheet_pattern)
                sku_frame = pd.concat([sku_frame, iter_frame], ignore_index=True)
        sku_frame = self.sku_a_replace(sku_frame)
        return list(set(sku_frame.SKU))

    def get_inv_num(self,sku_list):
        url = input('请手动修改API信息后重新运行') # 实时计算数据接口
        data = ''
        res = requests.post(url, data=data)
        if res.status_code == 200:
            if not res.json()['code'] == 200: # 正常响应，返回错误
                print(res.json())
                # 从SKU列表中删除不存在信息，并重新发起请求
                sku_list.remove(re.search('ItemNum(.+?)不存在', res.json()['message']).group(1).strip())
                return self.get_inv_num(sku_list)
            json.dump(self.parse_json(res),open(os.path.join(self.file_path,self.file_settings['mfn_inv']),'w'))
        else:
            return self.get_inv_num(sku_list)

    def parse_json(self,response):
        '''解析库存结果，选出结果中Warehouse为LA的库存结果，并逐条dict删除StoreLocationLines键及其结果'''
        result = []
        js = response.json()['result']['dataResult']
        for item in js:
            item = item['InventoryModel']['InventoryDataTypes']
            item = [info for info in item if info['Warehouse'] == 'LA'][0]
            item = {key: item[key] for key in item.keys() if not key == 'StoreLocationLines'}
            result.append(item)
        return result

    def inv_num_query(self):
        if os.path.exists(os.path.join(self.file_path,self.file_settings['mfn_inv'])):return
        sku_list = self.get_sku_list()
        self.get_inv_num(sku_list)
        print('库存数据已保存')

if __name__ == '__main__':
    i = InvNumQuery()
    i.inv_num_query()