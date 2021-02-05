# -*- coding: utf-8-sig -*-
"""
File Name ：    po_supply_time
Author :        Eric
Create date ：  2020/11/4

计算po补充的产品到岸后，LA剩余库存能够支持的剩余销售时间
"""

import numpy as np
import pandas as pd
import re
from fuzzywuzzy import process,fuzz

# def la_time_predict(single_sku, combo_frame):
#     '''
#     计算月平均LA剩余库存时间
#     single_sku 为单品sku
#     combo_frame 为包含此单品的combo与inventory数据联结的表格
#     '''
#     combo_frame = combo_frame.drop_duplicates(subset=['SKU'])
#     combo_frame.sort_values(by=['Prime库存时间', 'WBR'], ascending=[True, False], inplace=True)
#     inv_time = 0
#     wbr_total = 0
#     la_inv = float(combo_frame.loc[combo_frame['SKU'] == single_sku, 'LA库存'])
#     for i, sku_iter in enumerate(combo_frame['SKU'].tolist()):
#
#         prime_time = float(combo_frame.loc[combo_frame['SKU'] == sku_iter, 'Prime库存时间'])
#
#         wbr_iter = float(combo_frame.loc[combo_frame['SKU'] == sku_iter, 'WBR'])
#         weight = float(combo_frame.loc[combo_frame['SKU'] == sku_iter, 'weight'])
#
#         pre_time = la_inv / (wbr_iter * weight + wbr_total) if (wbr_iter * weight + wbr_total) > 0 else 0 # 计算加入当前产品后la剩余库存时间
#         inv_left = la_inv - ((prime_time) * (wbr_iter * weight + wbr_total))  # 计算库存剩余
#
#         if i == 0:  # 起点
#             inv_time += prime_time
#             wbr_total += (wbr_iter * weight)
#             continue
#
#         if inv_left > 0:
#             # 库存有剩余，继续参与计算
#             la_inv = inv_left
#             inv_time = prime_time
#             wbr_total += (wbr_iter * weight)
#
#         else:
#             inv_time += pre_time
#             return inv_time
#     else:
#         # 库存能够支撑到所有prime断货
#         left_time = (la_inv / wbr_total if wbr_total > 0 else np.inf) + inv_time
#         return inv_time + left_time

def la_time_predict( combo_frame):
    '''
    计算即时剩余库存时间
    combo_frame 为包含此单品的combo与inventory数据联结的表格
    '''

    for column in ['SKU', 'Single', 'weight', 'LA库存', 'Prime库存时间', 'WBR_Retail', 'WBR']:
        if not column in combo_frame.columns: raise KeyError(f'缺少指定列 ：{column}')
    combo_frame['Prime库存时间'] = combo_frame['Prime库存时间'].replace(np.inf, 0).replace(np.nan, 0)
    la_inv = float(combo_frame.loc[combo_frame['SKU'] == combo_frame['Single'], 'LA库存'])
    inv_list = [la_inv, ]

    prime_time_stage = sorted(list(set(combo_frame['Prime库存时间'])), reverse=False)
    combo_frame['wbr_delta'] = (combo_frame['WBR'] - combo_frame['WBR_Retail']) * combo_frame[
        'weight']  # 差值为各自对应的wbr差值
    combo_frame.loc[combo_frame['wbr_delta'] < 0, 'wbr_delta'] = 0  # 负值填充为0
    combo_frame['WBR_Retail'] *= combo_frame['weight']  # 按比例放大

    for i, prime_time in enumerate(prime_time_stage):
        # 剩余期望库存
        inv_left = la_inv - (combo_frame['wbr_delta'] * prime_time).sum()  # LA消耗
        inv_left -= ((prime_time - combo_frame.loc[combo_frame['Prime库存时间'] <= prime_time, 'Prime库存时间']) *
                     combo_frame.loc[combo_frame['Prime库存时间'] <= prime_time, 'WBR_Retail']).sum()  # Prime补充
        inv_list.append(inv_left)

        if inv_left < 0:
            inv_left = inv_list[-2]  # 最近的未售完库存
            wbr_total = combo_frame['wbr_delta'].sum() + combo_frame.loc[
                combo_frame['Prime库存时间'] < prime_time, 'WBR_Retail'].sum()  # 库存将为0时的WBR之和
            pre_time = prime_time_stage[i - 1] + np.round((inv_left / wbr_total), decimals=2)  # 预期销售时间
            return pre_time

    else:  # 库存支撑到Prime售罄
        inv_left = inv_list[-1]
        wbr_total = (combo_frame['WBR'] * combo_frame['weight']).sum()
        return prime_time_stage[-1] + (inv_left / wbr_total)





path = r'C:\Users\Administrator\data\运营\02032021' #运营表路径
# 库存整理后的文件
report_file = r'C:\Users\Administrator\Documents\WeChat Files\wxid_3ugh8icd8zod22\FileStorage\File\2021-02\0204 - 副本.xlsx'
# 记录SKU对应关系的产品
combo_file = f'{path}\\Combo对应关系.xlsx'
# 所有商品的wbr数量
wbr_relation_file = f'{path}\\total.xlsx'
# 要保存到的文件名
target_file = f'{path}\\低库存结果.xlsx'

inv_rep_frame = pd.read_excel(report_file)
# 前八列自动填充空值
[inv_rep_frame[column].fillna(method='ffill',inplace=True) for column in inv_rep_frame.columns.to_list()[:8]]
# 汇总每个SKU补货的数量
po_total_frame = inv_rep_frame[['SKU','Number']].groupby('SKU').sum().merge(inv_rep_frame.drop_duplicates(subset=['SKU'],keep='first').drop(['Number'],axis=1),on='SKU',how='inner')

# 读SKU关系表
combo_reflec_frame = pd.read_excel(combo_file)
indep_inv_frame = pd.read_excel(combo_file,sheet_name='库存独立产品')

combo_set = set(combo_reflec_frame['ItemNum'])
single_set = set(combo_reflec_frame['ChildItemNum'])
# 独立库存产品中去掉参与组成combo的SKU
indep_inv_sku_set = set(indep_inv_frame['SKU'])
indep_inv_sku_set -= single_set

total_inv_frame = pd.read_excel(wbr_relation_file,sheet_name='inv')
wbr_frame = pd.read_excel(wbr_relation_file,sheet_name='wbr')
total_inv_frame = total_inv_frame.merge(wbr_frame[['ASIN','WBR_Retail']],on='ASIN',how='left')

po_total_frame['total_time'] = 0
for sku in po_total_frame['SKU'].tolist():
    if sku in indep_inv_sku_set:
        po_total_frame.loc[po_total_frame['SKU'] == sku, 'total_time'] = (po_total_frame.loc[
                                                                              po_total_frame['SKU'] == sku, 'LA库存'] +
                                                                          po_total_frame.loc[
                                                                              po_total_frame['SKU'] == sku, 'Number']) / \
                                                                         po_total_frame.loc[
                                                                             po_total_frame['SKU'] == sku, 'WBR']
        continue

    combo_iter = combo_reflec_frame[combo_reflec_frame['ChildItemNum'] == sku]
    combo_iter = combo_iter.append({'ItemNum': sku, 'ChildItemNum': sku, 'ChildQty': 1}, ignore_index=True)
    combo_iter.columns = ['SKU', 'Single', 'weight']

    combo_iter = combo_iter.merge(total_inv_frame, on='SKU', how='inner')
    combo_iter.loc[combo_iter['SKU'] == sku, 'LA库存'] = float(
        po_total_frame.loc[po_total_frame['SKU'] == sku, 'Number'] + po_total_frame.loc[
            po_total_frame['SKU'] == sku, 'LA库存'])

    combo_iter['WBR_Total'].fillna(0,inplace=True)
    inv_time = la_time_predict(combo_iter)

    po_total_frame.loc[po_total_frame['SKU'] == sku, 'total_time'] = inv_time


inv_rep_frame = inv_rep_frame.merge(po_total_frame[['SKU','total_time']])
# 排序
inv_rep_frame = inv_rep_frame[['ProductLine','SKU','LA库存','WBR','WBR_Total','Prime库存时间','即时库存时间','total_time','LA库存时间',
                               'Number','PO','CTNR#','ETD','ETA','Notes','出货工厂']]
inv_rep_frame.rename(columns={'total_time':'总库存时间'},inplace=True)

index = pd.MultiIndex.from_frame(inv_rep_frame[['ProductLine','SKU','LA库存','WBR','WBR_Total','Prime库存时间','即时库存时间','LA库存时间','总库存时间','Number']])
inv_rep_frame.drop(['ProductLine','SKU','LA库存','WBR','WBR_Total','Prime库存时间','即时库存时间','LA库存时间','总库存时间','Number'],inplace=True,axis=1)
inv_rep_frame.index = index
inv_rep_frame.to_excel(target_file)



