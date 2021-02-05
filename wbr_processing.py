# -*- coding: utf-8-sig -*-
"""
File Name ：    wbr_processing
Author :        Eric
Create date ：  2021/1/28

------------------------------------------------- 1.28更新 -------------------------------------------------------------
* 修改api merge_wbr_frame 为 concat_wbr_frame

"""

import pandas as pd
import xlwt
import xlrd
from xlutils.copy import copy
from fuzzywuzzy import fuzz
import numpy as np
from setting import Setting
import re

#
# class WbrDataProcess(Setting):
#     def __init__(self):
#         super().__init__()
#         self.excel_file_list = [self.file_settings['file_path'] + file for file in self.file_settings['merchand_files_list']]
#         self.wbr_sheet = self.sheet_settings['wbr_sheet']
#         self.wbr_sheet_dic = {}
#         self.inv_sheet_dic = {} # 记录运营表数据所在sheet
#
#     def read_wbr_sheet(self,file, wbr_sheet, drop_columns_num=0):  # 表最后一列备注删除
#         '''读取运营表中wbr相关数据'''
#         excel_file = pd.ExcelFile(file)
#         # 记录运营表相关sheet
#         self.inv_sheet_dic[file] = []
#         [self.inv_sheet_dic[file].append(sheet) for sheet in excel_file.sheet_names if re.search(self.sheet_settings['inv_sheet_pattern'],sheet)]
#         if len(self.inv_sheet_dic.get(file)) == 0:
#             [self.inv_sheet_dic[file].append(sheet) for sheet in excel_file.sheet_names if
#              re.search(self.sheet_settings['inv_spare_sheet_pattern'], sheet)]
#         # 查找wbr sheet
#         chance_dic = {}
#         for sheetname in excel_file.sheet_names:
#             chance_dic[sheetname] = fuzz.ratio(wbr_sheet, sheetname)
#         sheet, chance = '', 0
#         for key, value in chance_dic.items():
#             if value > chance:
#                 sheet, chance = key, value
#
#         # 记录每个文件对应的 wbr sheet 名称
#         self.wbr_sheet_dic[file] = sheet
#         wbr = pd.DataFrame(pd.read_excel(file, sheet_name=sheet))
#         columns_list = wbr.columns
#         for i in range(-drop_columns_num, 0):
#             wbr.drop(columns_list[i], axis=1, inplace=True)
#         columns_list = wbr.columns
#
#         wbr = wbr[['ASIN',
#                    'SKU',
#                    columns_list[-3],
#                    columns_list[-2],
#                    columns_list[-1]]]
#         wbr.columns = ['ASIN', 'SKU', 'd1', 'd2', 'd3']
#         wbr.drop(0, inplace=True)
#         for item in wbr.columns.tolist()[2:]:
#             wbr[item].fillna(0, inplace=True)
#
#         # 验证是否为正常数值列，勿删！！！
#         try:  # 逐列递归，删除不为数值的列
#             wbr['wbr'] = (wbr['d1'] + wbr['d2'] + wbr['d3']) / 3
#         except BaseException:
#             return self.read_wbr_sheet(file, sheet, drop_columns_num + 1)
#         wbr['d4'] = 0  # 要求值的新增列
#         wbr = wbr[['ASIN', 'SKU', 'd1', 'd2', 'd3', 'd4']]  # 排序
#         return wbr
#
#
#     # ----------------------------------------------------------------------------------------------------------
#     # wbr 相关文件中销售数据读取
#     def read_wbr_retail(self,file):
#         '''
#         读 Retail WBR值
#         '''
#         wbr_retail = pd.read_excel(file)
#         try:  # 判断wbr——retail表第一行为标题行或空行
#             wbr_retail['ASIN']
#         except BaseException:
#             wbr_retail.columns = wbr_retail.iloc[0]
#             wbr_retail.drop(0, inplace=True)
#         wbr_retail = wbr_retail[self.wbr_column_settings['retail_columns']]
#         wbr_retail.rename(columns=self.wbr_column_settings['retail_columns_rename'], inplace=True)
#         return wbr_retail
#
#     def read_wbr_mfn(self,file):
#         '''读取本周mfn的wbr数据'''
#         try:
#             wbr_mfn = pd.DataFrame(pd.read_csv(file, sep='\t',encoding='utf-8-sig'))
#             # 验证文件列分隔符是否为'\t'
#             wbr_mfn[self.wbr_column_settings['mfn_channel_column_name']].apply(
#                 lambda x: x in self.wbr_column_settings['mfn_channel_column_value'])
#         except:
#             wbr_mfn = pd.DataFrame(pd.read_csv(file, sep=',',encoding='utf-8-sig'))
#         # 保留channel字段为指定值的行
#         wbr_mfn = wbr_mfn[wbr_mfn[self.wbr_column_settings['mfn_channel_column_name']].apply(
#             lambda x: x in self.wbr_column_settings['mfn_channel_column_value'])]
#         # 删除status字段为指定值的行
#         wbr_mfn = wbr_mfn[~(wbr_mfn[self.wbr_column_settings['mfn_status_column_name']].apply(
#             lambda x: x in self.wbr_column_settings['mfn_status_column_value']))]
#         # 删除fulfillment字段为指定值的行
#         wbr_mfn = wbr_mfn[~(wbr_mfn[self.wbr_column_settings['mfn_fulfillment_column_name']].apply(
#             lambda x: x in self.wbr_column_settings['mfn_fulfillment_column_value']))]
#
#         wbr_mfn = wbr_mfn[self.wbr_column_settings['mfn_columns']]
#         wbr_mfn.rename(columns=self.wbr_column_settings['mfn_columns_rename'], inplace=True)
#         wbr_mfn = wbr_mfn.groupby(self.wbr_column_settings['mfn_aggre_column']).sum()
#         return wbr_mfn
#         # 合并上周最新WBR数据并求和
#
#     def read_wbr_retail_xq(self,file):
#         wbr_retail_xq = pd.DataFrame(pd.read_csv(file, encoding='utf-8-sig'))  # 读 wbr_xq
#         wbr_retail_xq = wbr_retail_xq[self.wbr_column_settings['retail_xq_columns']]  # 保留指定列
#         wbr_retail_xq = wbr_retail_xq.rename(columns=self.wbr_column_settings['retail_xq_columns_rename'])  # 列重命名
#         wbr_retail_xq = wbr_retail_xq.groupby(self.wbr_column_settings['retail_xq_aggre_column']).sum()  # 聚合
#         return wbr_retail_xq
#
#     def concat_wbr_frame(self, ):
#         '''
#         读wbr表格ASIN、SKU及最后三周wbr数值
#         '''
#         wbr_frame = pd.DataFrame()
#         for file in self.excel_file_list:
#             wbr_iter_frame = self.read_wbr_sheet(file, self.wbr_sheet)
#             wbr_frame = pd.concat([wbr_frame, wbr_iter_frame], ignore_index=True)
#         wbr_frame.drop_duplicates(subset=['SKU'], inplace=True)  # SKU 去重
#         wbr_frame.drop_duplicates(subset=['ASIN'], inplace=True)  # SKU 去重
#         return wbr_frame
#
#     def wbr_latest_number(self):
#         '''
#         计算上周的平均wbr销量
#         '''
#         file_path = self.file_settings['file_path']
#         xq_file = file_path + '/' + self.file_settings['retial_xq_wbr']
#         retail_file = file_path + '/' + self.file_settings['retail_wbr']
#         mfn_file = file_path + '/' + self.file_settings['mfn_wbr']
#
#         total_wbr_frame = self.concat_wbr_frame()
#         xq_wbr = self.read_wbr_retail_xq(xq_file)
#         retail_wbr = self.read_wbr_retail(retail_file)
#         mfn_wbr = self.read_wbr_mfn(mfn_file)
#         # 多表合并
#         total_wbr_frame = total_wbr_frame.merge(retail_wbr,
#                                                 on='ASIN', how='left').merge(mfn_wbr, on='ASIN', how='left')\
#             .merge(xq_wbr, on='ASIN', how='left')
#         total_wbr_frame[self.wbr_column_settings['wbr_la_column']] = total_wbr_frame[
#             self.wbr_column_settings['wbr_la_detail_columns']].sum(axis=1)
#         total_wbr_frame[self.wbr_column_settings['wbr_last_week_column']] = total_wbr_frame[
#             self.wbr_column_settings['wbr_total_columns']].sum(axis=1)
#
#         total_wbr_frame[self.wbr_column_settings['wbr_weekly_avg_column']] = (
#                     total_wbr_frame[self.wbr_column_settings['wbr_weekly_avg_detail_columns']].sum(
#                         axis=1) / 4).round(decimals=2)
#         return total_wbr_frame
#         # 结果保存
#
#     def save_wbr(self, target_file):
#         '''
#         将wbr数据保存到目标文件夹
#         '''
#         total_wbr_frame = self.total_wbr_frame.copy()
#         total_wbr_frame.drop(self.wbr_column_settings['wbr_dropped_columns'], axis=1, inplace=True)
#         excel_writer = pd.ExcelWriter(target_file)
#         # 读取每个运营表中wbr sheet对应的数据并保存
#
#         for file, sheet in self.wbr_sheet_dic.items():
#             writer_sheet = ''.join(file.split('/')[-1].split('.')[:-1])[:20]
#             wbr_frame = self.read_wbr_sheet(file, sheet)
#             wbr_frame.drop('d4', axis=1, inplace=True)
#             wbr_frame = wbr_frame.merge(total_wbr_frame[['SKU', 'd4', 'wbr']], on='SKU', how='left')
#
#             wbr_frame.to_excel(excel_writer, sheet_name=writer_sheet, index=False)
#         excel_writer.save()
#
#         # ------------------------------------------------执行函数------------------------------------------------------
#
#     def wbr_data_processing(self):
#         self.total_wbr_frame = self.wbr_latest_number()
#         self.save_wbr(self.file_settings['result_path'] + '/' + 'wbr_result.xlsx')
#         self.total_wbr_frame.to_excel(self.file_settings['file_path'] + '/' + self.file_settings['total_wbr'],
#                                       index=False)

class WbrDataProcess(Setting):
    def __init__(self):
        super().__init__()
        # self.file_settings['file_path'] = r'C:\Users\Administrator\data\运营\10202020/'
        self.excel_file_list = [self.file_settings['file_path'] + file for file in self.file_settings['merchand_files_list']]
        self.wbr_sheet = self.sheet_settings['wbr_sheet']
        self.wbr_sheet_dic = {}
        self.inv_sheet_dic = {} # 记录运营表数据所在sheet

    def read_wbr_sheet(self,file, wbr_sheet, drop_columns_num=0):  # 表最后一列备注删除
        excel_file = pd.ExcelFile(file)
        # 记录运营表相关sheet
        self.inv_sheet_dic[file] = []
        [self.inv_sheet_dic[file].append(sheet) for sheet in excel_file.sheet_names if re.search(self.sheet_settings['inv_sheet_pattern'],sheet)]
        if len(self.inv_sheet_dic.get(file)) == 0:
            [self.inv_sheet_dic[file].append(sheet) for sheet in excel_file.sheet_names if
             re.search(self.sheet_settings['inv_spare_sheet_pattern'], sheet)]
            # chance,inv_sheet = 0,''
            # for sheet in excel_file.sheet_names:
            #     chance,inv_sheet = (fuzz.ratio(sheet,self.sheet_settings['inv_spare_sheet_pattern']),sheet) if chance < fuzz.partial_ratio(sheet,self.sheet_settings['inv_spare_sheet_pattern']) else (chance,inv_sheet)

            # self.inv_sheet_dic[file].append(inv_sheet)

        # 寻找最近的wbr sheet name
        chance_dic = {}
        for sheetname in excel_file.sheet_names:
            chance_dic[sheetname] = fuzz.ratio(wbr_sheet, sheetname)

        sheet, chance = '', 0
        for key, value in chance_dic.items():
            if value > chance:
                sheet, chance = key, value

        self.wbr_sheet_dic[file] = sheet # 记录每个文件对应的 wbr sheet 名称

        wbr = pd.DataFrame(pd.read_excel(file, sheet_name=sheet))
        columns_list = wbr.columns
        for i in range(-drop_columns_num, 0):
            wbr.drop(columns_list[i], axis=1, inplace=True)
        columns_list = wbr.columns

        wbr = wbr[['ASIN',
                   'SKU',
                   columns_list[-3],
                   columns_list[-2],
                   columns_list[-1]]]

        wbr.columns = ['ASIN', 'SKU', 'd1', 'd2', 'd3']
        wbr.drop(0, inplace=True)
        for item in wbr.columns.to_list()[2:]:
            wbr[item].fillna(0, inplace=True)

        # 验证是否为正常数值列，勿删！！！
        try:  # 逐列递归，删除不为数值的列
            wbr['wbr'] = (wbr['d1'] + wbr['d2'] + wbr['d3']) / 3
        except BaseException:
            return self.read_wbr_sheet(file, sheet, drop_columns_num + 1)
        wbr['d4'] = 0  # 要求值的新增列
        wbr = wbr[['ASIN', 'SKU', 'd1', 'd2', 'd3', 'd4']]  # 排序
        return wbr

    # wbr 相关文件中销售数据读取
    # ----------------------------------------------------------------------------------------------------------
    def read_wbr_retail(self,file):
        '''
        读 Retail WBR值，及Retail_xq wbr数值
        '''
        wbr_retail = pd.read_excel(file)

        try:  # 判断wbr——retail表第一行为标题行或空行
            wbr_retail['ASIN']
        except BaseException:
            wbr_retail.columns = wbr_retail.iloc[0]
            wbr_retail.drop(0, inplace=True)

        wbr_retail = wbr_retail[self.wbr_column_settings['retail_columns']]
        wbr_retail.rename(columns=self.wbr_column_settings['retail_columns_rename'], inplace=True)
        return wbr_retail


    def read_wbr_mfn(self,file):
        try:
            wbr_mfn = pd.DataFrame(pd.read_csv(file, sep='\t',encoding='utf-8-sig'))
            # 验证文件列分隔符是否为'\t'
            wbr_mfn[self.wbr_column_settings['mfn_channel_column_name']].apply(
                lambda x: x in self.wbr_column_settings[
                    'mfn_channel_column_value'])

        except:
            wbr_mfn = pd.DataFrame(pd.read_csv(file, sep=',',encoding='utf-8-sig'))
        # 保留channel字段为指定值的行
        wbr_mfn = wbr_mfn[wbr_mfn[self.wbr_column_settings['mfn_channel_column_name']].apply(
            lambda x: x in self.wbr_column_settings['mfn_channel_column_value'])]
        # 删除status字段为指定值的行
        wbr_mfn = wbr_mfn[~(wbr_mfn[self.wbr_column_settings['mfn_status_column_name']].apply(
            lambda x: x in self.wbr_column_settings['mfn_status_column_value']))]
        # 删除fulfillment字段为指定值的行
        wbr_mfn = wbr_mfn[~(wbr_mfn[self.wbr_column_settings['mfn_fulfillment_column_name']].apply(
            lambda x: x in self.wbr_column_settings['mfn_fulfillment_column_value']))]

        wbr_mfn = wbr_mfn[self.wbr_column_settings['mfn_columns']]
        wbr_mfn.rename(columns=self.wbr_column_settings['mfn_columns_rename'], inplace=True)
        wbr_mfn = wbr_mfn.groupby(self.wbr_column_settings['mfn_aggre_column']).sum()
        return wbr_mfn

    def read_wbr_retail_xq(self,file):
        wbr_retail_xq = pd.DataFrame(pd.read_csv(file, encoding='utf-8-sig'))  # 读 wbr_xq
        wbr_retail_xq = wbr_retail_xq[self.wbr_column_settings['retail_xq_columns']]  # 保留指定列
        wbr_retail_xq = wbr_retail_xq.rename(columns=self.wbr_column_settings['retail_xq_columns_rename'])  # 列重命名
        wbr_retail_xq = wbr_retail_xq.groupby(self.wbr_column_settings['retail_xq_aggre_column']).sum()  # 聚合
        return wbr_retail_xq

    #-----------------------------------------------------------------------------------------------------------------
    # 合并上周最新WBR数据并求和
    def merge_wbr_frame(self,):
        '''
        读wbr表格ASIN、SKU及最后三周wbr数值
        '''
        wbr_frame = pd.DataFrame()

        for file in self.excel_file_list:
            wbr_iter_frame = self.read_wbr_sheet(file, self.wbr_sheet)
            wbr_frame = pd.concat([wbr_frame, wbr_iter_frame], ignore_index=True)

        wbr_frame.drop_duplicates(subset=['SKU'], inplace=True)  # SKU 去重
        wbr_frame.drop_duplicates(subset=['ASIN'], inplace=True)  # SKU 去重
        return wbr_frame


    def wbr_latest_number(self):
        '''
        计算上周的平均wbr销量
        '''
        file_path = self.file_settings['file_path']
        xq_file = file_path + '/' + self.file_settings['retial_xq_wbr']
        retail_file = file_path + '/' + self.file_settings['retail_wbr']
        mfn_file = file_path + '/' + self.file_settings['mfn_wbr']

        total_wbr_frame = self.merge_wbr_frame()
        xq_wbr = self.read_wbr_retail_xq(xq_file)
        retail_wbr = self.read_wbr_retail(retail_file)
        mfn_wbr = self.read_wbr_mfn(mfn_file)

        # 多表合并
        total_wbr_frame = total_wbr_frame.merge(retail_wbr,
                                                on='ASIN', how='left').merge(mfn_wbr, on='ASIN', how='left').merge(
            xq_wbr, on='ASIN', how='left')

        total_wbr_frame[self.wbr_column_settings['wbr_la_column']] = total_wbr_frame[
            self.wbr_column_settings['wbr_la_detail_columns']].sum(axis=1)
        total_wbr_frame[self.wbr_column_settings['wbr_last_week_column']] = total_wbr_frame[
            self.wbr_column_settings['wbr_total_columns']].sum(axis=1)

        total_wbr_frame[self.wbr_column_settings['wbr_weekly_avg_column']] = (
                    total_wbr_frame[self.wbr_column_settings['wbr_weekly_avg_detail_columns']].sum(
                        axis=1) / 4).round(decimals=2)
        return total_wbr_frame


    # 结果保存
    def save_wbr(self, target_file):
        '''
        将wbr数据保存到目标文件夹
        '''
        total_wbr_frame = self.total_wbr_frame.copy()
        total_wbr_frame.drop(self.wbr_column_settings['wbr_dropped_columns'], axis=1, inplace=True)
        excel_writer = pd.ExcelWriter(target_file)
        # 读取每个运营表中wbr sheet对应的数据并保存

        for file,sheet in self.wbr_sheet_dic.items():
            writer_sheet = ''.join(file.split('/')[-1].split('.')[:-1])[:20]
            wbr_frame = self.read_wbr_sheet(file,sheet)
            wbr_frame.drop('d4',axis=1,inplace=True)
            wbr_frame = wbr_frame.merge(total_wbr_frame[['SKU','d4','wbr']], on='SKU', how='left')

            wbr_frame.to_excel(excel_writer, sheet_name=writer_sheet, index=False)
        excel_writer.save()


    # ------------------------------------------------执行函数------------------------------------------------------
    def wbr_data_processing(self):
        self.total_wbr_frame = self.wbr_latest_number()
        self.save_wbr(self.file_settings['result_path'] +'/'+ 'wbr_result.xlsx')
        self.total_wbr_frame.to_excel(self.file_settings['file_path'] +'/'+self.file_settings['total_wbr'],index=False)


if __name__ == '__main__':

    w = WbrDataProcess()
    w.wbr_data_processing()

