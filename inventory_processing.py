# -*- coding: utf-8-sig -*-
"""
File Name ：    inventory_processing
Author :        Eric
Create date ：  2021/1/29
"""

import os
import xlrd
import xlwt
from xlutils.copy import copy
import time
from wbr_processing import WbrDataProcess
from inv_num_get import InvNumQuery
import pandas as pd
import re
import numpy as np
import warnings
warnings.filterwarnings('ignore')


def time_decorator(func):
    def call_func(*args):
        t1 = time.time()
        func(*args)
        t2 = time.time()
        print('%s 运行时间为 %d 分 %d 秒'%(func.__name__,(t2 - t1)//60, (t2-t1)%60))
    return call_func

class InvDataProcess(WbrDataProcess,InvNumQuery):
    def __init__(self):
        super().__init__()
        self.combo_file = self.file_settings['file_path'] + '/' + self.file_settings['combo_inv']

    def read_inv_file(self):
        '''
        依次读取inv_sheet_dic中记录的所有文件中记录的inv相关sheet，并以{file:{sheet:data}}形式返回
        '''
        sheet_dict = self.inv_sheet_dic
        file_list = sheet_dict.keys()
        # 记录所有文件及其sheet
        inv_frame_dict = {}
        # 合并后的容器
        totoal_inv_frame = pd.DataFrame()

        # 读运营表中有效sheet中数据并保存到字典
        for file in file_list:
            file_name = ''.join(file.split('/')[-1].split('.')[:-1])
            # 每一文件内对应sheet
            inv_frame_dict[file_name] = {}

            for sheet in sheet_dict[file]:
                inv_frame = pd.read_excel(file, sheet_name=sheet, header=1)
                inv_frame.columns = [str(x).strip().replace(' ', '') for x in inv_frame.columns.tolist()] # 去掉所有column中空格

                inv_frame = inv_frame[self.inv_column_settings['inv_frame_column']]
                if self.inv_column_settings['frame_filled']:  # 填充指定列空值
                    [inv_frame[column].fillna(method=self.inv_column_settings['frame_filled']['fill_method'],
                                              inplace=True) for column in
                     self.inv_column_settings['frame_filled']['columns']]

                for column in inv_frame.columns.tolist():
                    inv_frame[column] = inv_frame[column].str.strip().str.replace('\r', '').str.replace('\t',
                        '').str.replace('\n', '')

                inv_frame.reset_index(inplace=True, drop=True)
                inv_frame_dict[file_name][sheet] = inv_frame
                totoal_inv_frame = pd.concat([totoal_inv_frame, inv_frame], ignore_index=True)

        # 去掉无效的行
        totoal_inv_frame = totoal_inv_frame[
            totoal_inv_frame[self.inv_column_settings['inv_asin_filter_conditon']['filter_column']].apply(
                lambda x: True if re.search(
                    self.inv_column_settings['inv_asin_filter_conditon']['filter_condition'], str(x)) else False)]
        inv_frame_dict[self.inv_column_settings['inv_concat']] = totoal_inv_frame  # 记录合并后的运营表数据
        return inv_frame_dict

    # ---------------------------------------------读相关数据-----------------------------------------------------------
    def read_bb_inv(self,file):
        bb = pd.read_csv(file)
        bb = bb[self.bb_column_settings['bb_inv_columns']]
        bb.rename(columns=self.bb_column_settings['bb_inv_column_rename'], inplace=True)
        bb = bb.groupby(self.bb_column_settings['bb_aggre_column']).max()
        return bb

    def read_mfn_inv(self,file):
        # mfn = pd.read_excel(file)
        mfn = pd.read_json(file)
        mfn = mfn[self.mfn_column_settings['mfn_inv_columns']]
        mfn.rename(columns=self.mfn_column_settings['mfn_inv_column_rename'], inplace=True)
        mfn.set_index(self.mfn_column_settings['mfn_index_column'], inplace=True)

        for column in self.mfn_column_settings['mfn_zero_fill_column']:
            mfn[mfn[column] < 0] = 0
        return mfn


    def read_retail_po_inv(self,file):
        '''
        读retail文件，返回retail 及 po细节 要保留的数据所在的两个dataframe
        '''
        retail_po = pd.DataFrame(pd.read_excel(file))  # 读retail和openPO数据表路径
        # 判断 表格标头位于第一行或第二行
        try:
            retail = retail_po[self.retail_column_settings['retail_inv_columns']]
        except BaseException:
            retail_po.columns = retail_po.iloc[0].to_list()
            retail_po = retail_po.drop(0)
            retail = retail_po[self.retail_column_settings['retail_inv_columns']]
        retail.rename(columns=self.retail_column_settings['retail_inv_column_rename'],inplace=True)
        retail.set_index(self.retail_column_settings['retail_index_column'], inplace=True)
        # po 细节处理
        open_po = retail_po[self.retail_column_settings['po_inv_columns']]
        open_po.rename(columns=self.retail_column_settings['po_inv_column_rename'], inplace=True)
        open_po.set_index(self.retail_column_settings['po_index_column'], inplace=True)
        return retail, open_po

    def read_combo(self,file):
        '''读combo文件，保留指定列'''
        combo = pd.read_excel(file, sheet_name=self.sheet_settings['combo_reflection_sheet'])
        combo = combo[self.combo_column_settings['combo_relation_column']]
        combo.rename(columns=self.combo_column_settings['combo_relation_column_rename'],inplace=True)
        combo.drop_duplicates(inplace=True)
        return combo

    # ---------------------------------------------处理相关数据-----------------------------------------------------------
    @time_decorator
    def inv_data_processing(self):
        '''
        读取数据，并进行相应处理，并将结果保存到指定文件中
        '''
        self.inv_num_query()
        self.wbr_data_processing()
        self.inv_frame_dic = self.read_inv_file()
        self.total_inv_frame = self.inv_frame_dic.pop(self.inv_column_settings['inv_concat'])
        self.inv_merge_data()
        # 将 WBR数据 与 inv数据 并联

        self.total_inv_frame = self.total_inv_frame.merge(self.total_wbr_frame[[self.data_processing_setting['wbr_merge_column']]
                                                             +  self.data_processing_setting[
                                                                    'wbr_effective_columns']],
                                                on=self.data_processing_setting['wbr_merge_column'],
                                                how='left').fillna(0)
        self.total_inv_frame.drop_duplicates(inplace=True, keep='first')
        for column in self.inv_column_settings['converse_float_columns']: self.total_inv_frame[column] = self.total_inv_frame[column].astype('float')
        # 计算primetime
        self.total_inv_frame[self.data_processing_setting['pt_column']] = \
            self.total_inv_frame[self.data_processing_setting['pt_numerator_columns']].sum(axis=1)/\
            self.total_inv_frame[self.data_processing_setting['pr_denominator_columns']].sum(axis=1)\
            .replace(np.nan, 0).replace(np.inf, 0).round(decimals=2)

        for column in ['LATime','SingleTime','SingleLAWbr','LAverageTime','SingleWBRbyMonth']:
            self.total_inv_frame[column] = 0

        self.single_inv_time_cal()
        self.combo_inv_time_cal()

        self.save_data_to_excel()
        self.low_time_product_select() # 低库存
        # 汇总结果保存
        total_data_writer = pd.ExcelWriter(os.path.join(self.file_settings['file_path'],'total.xlsx'))
        self.total_inv_frame.to_excel(total_data_writer,sheet_name='inv', index=False)
        self.total_wbr_frame.to_excel(total_data_writer,sheet_name='wbr', index=False)
        total_data_writer.save()
        # self.wbr_compare()

    def single_inv_time_cal(self):
        '''
        根据LA销量及Retail断货后的需求量造成的LA库存消耗，对单品的LA库存时间进行估算
        '''
        self.combo_frame = self.read_combo(self.combo_file)
        total_inventory_frame = self.total_inv_frame.copy()
        independent_sku_list = pd.DataFrame(
            pd.read_excel(self.combo_file,
                sheet_name='库存独立产品'))['SKU'].tolist()
        single_list = self.combo_frame['Single'].drop_duplicates().tolist()
        self.single_list = list(set(single_list) | set(independent_sku_list))
        combo_list = self.combo_frame['Combo'].drop_duplicates().tolist()
        self.combo_list = list(set(combo_list) - set(independent_sku_list))
        self.sku_list = total_inventory_frame['SKU'].tolist() #运营表中所有产品的sku

        for index in total_inventory_frame.index:
            sku = total_inventory_frame.loc[index, 'SKU']
            if sku in combo_list or not sku.strip():  # 跳过combo或sku为空的行
                continue

            # 将关联到的Combo信息中不在sku list中的combo全部过滤掉
            combo_iter_frame = self.combo_frame[self.combo_frame['Single'] == sku] \
                [self.combo_frame[self.combo_frame['Single'] == sku]['Combo'].apply(lambda x: True if x in self.sku_list else False)]
            combo_iter_frame = combo_iter_frame.append({'Combo': sku, 'Single': sku, 'weight': 1}, ignore_index=True)

            # 关联Combo与库存信息
            combo_iter_frame = combo_iter_frame.rename(columns={'Combo': 'SKU'})
            combo_iter_frame = combo_iter_frame.merge(total_inventory_frame, on='SKU', how='left')
            # 计算Combo与单品关联后的销量之和
            wbr_la = (combo_iter_frame['WBR_LA'] * combo_iter_frame['weight']).sum()
            wbr_monyhly = (combo_iter_frame['wbr'] * combo_iter_frame['weight']).sum()

            # 将值记录到表格中
            total_inventory_frame.loc[index, 'SingleLAWbr'] = wbr_la
            total_inventory_frame.loc[index, 'SingleWBRbyMonth'] = wbr_monyhly

            single_la_time = np.round(
                self.imm_la_time_predict(combo_iter_frame),
                decimals=2)
            total_inventory_frame.loc[index, 'SingleTime'] = single_la_time
            total_inventory_frame.loc[index, 'LATime'] = single_la_time

            la_monthly_time = np.round(
                self.la_time_predict(combo_iter_frame),
                2)
            total_inventory_frame.loc[index, 'LAverageTime'] = la_monthly_time
        self.total_inv_frame = total_inventory_frame.copy()


    def combo_inv_time_cal(self):
        '''
        根据单品剩余库存时间，选择最低的单品库存时间作为该系列Combo的库存时间
        将对应列重命名
        保留指定列
        修改指定列数据格式及数据长度
        '''
        total_inventory_frame = self.total_inv_frame.copy()
        for index in total_inventory_frame.index:
            sku = total_inventory_frame.loc[index, 'SKU']
            if sku not in self.combo_list:continue
            combo_iter_frame = self.combo_frame[self.combo_frame['Combo'] == sku]  # 默认组成combo的所有单品均在sku_list中
            combo_iter_frame = combo_iter_frame.rename(columns={'Single': 'SKU'})
            combo_iter_frame = combo_iter_frame.merge(
                total_inventory_frame, on='SKU', how='left')

            combo_iter_frame.sort_values(by=['LATime', 'weight'], ascending=[
                True, False], inplace=True)  # 按库存时间、权重排序，
            min_la_time = combo_iter_frame.iloc[0]['LATime']
            min_single_sku = combo_iter_frame.iloc[0]['SKU']  # 消耗最快的产品SKU

            if total_inventory_frame.loc[total_inventory_frame['SKU'] == min_single_sku].empty:
                total_inventory_frame.loc[index, 'SingleLAWbr'] = np.nan
                total_inventory_frame.loc[index, 'SingleWBRbyMonth'] = np.nan
                continue
            combo_iter_frame.sort_values(by=['LAverageTime', 'weight'], ascending=[
                True, False], inplace=True)
            min_la_average_time = combo_iter_frame.iloc[0]['LAverageTime']
            total_inventory_frame.loc[index, 'LATime'] = min_la_time  # 单品当前周消耗预计剩余库存时间

            total_monthly_time = min_la_average_time
            total_inventory_frame.loc[index, 'LAverageTime'] = total_monthly_time
            # Combo 的剩余库存时间由其所有单品中影响因子最大的决定
            total_inventory_frame.loc[index, 'SingleLAWbr'] = float(
                    total_inventory_frame.loc[total_inventory_frame['SKU'] == min_single_sku, 'SingleLAWbr'])

            total_inventory_frame.loc[index, 'SingleWBRbyMonth'] = np.nan  # 跳过非单品的WBR和值
        total_inventory_frame.rename(
            columns={
                'MFN': 'LA库存',
                'PrimeTime': 'Prime库存时间',
                'LATime': '即时库存时间',
                'wbr': 'WBR',
                'LAverageTime': 'LA库存时间',
                'SingleWBRbyMonth': 'WBR_Total'},
            inplace=True)
        total_inventory_frame = total_inventory_frame[['ProductLine',
                                                       'ASIN',
                                                       'SKU',
                                                       'BB%',
                                                       'LA库存',
                                                       'retail',
                                                       'open_po',
                                                       'Prime库存时间',
                                                       '即时库存时间',
                                                       'LA库存时间',
                                                       'WBR',
                                                       'WBR_Total', ]]
        total_inventory_frame['WBR'] = np.ceil(total_inventory_frame['WBR'])
        total_inventory_frame['WBR_Total'] = np.ceil(total_inventory_frame['WBR_Total'])
        for column in ['Prime库存时间','即时库存时间','LA库存时间',]:
            total_inventory_frame[column] = total_inventory_frame[column].replace(np.inf, np.nan).round(decimals=2)

        self.total_inv_frame = total_inventory_frame.copy()


    def save_data_to_excel(self):
        for file,sheet_dic in self.inv_frame_dic.items():
            # file 仅为去掉路径的文件名\

            excel_writer = pd.ExcelWriter(os.path.join(self.file_settings['result_path'],file+'_result.xlsx'))
            for sheet,inv_iter_frame in sheet_dic.items():
                inv_iter_frame = inv_iter_frame.merge(self.total_inv_frame.drop(['ProductLine','ASIN'],axis=1),on='SKU',how='left')
                inv_iter_frame.to_excel(excel_writer,sheet_name=sheet,index=False)
            excel_writer.save()



    def inv_merge_data(self):
        '''
        将运营表汇总后的列与bb、mfn、retail、po表中的数据合并
        '''
        file_path = self.file_settings['file_path']
        bb_file = file_path + '/' + self.file_settings['bb_inv']
        mfn_file = file_path + '/' + self.file_settings['mfn_inv']
        retail_po_file = file_path + '/' + self.file_settings['retial_po_inv']

        bb_frame = self.read_bb_inv(bb_file)
        mfn_frame = self.read_mfn_inv(mfn_file)
        retail_frame, po_frame = self.read_retail_po_inv(retail_po_file)

        self.total_inv_frame = self.total_inv_frame.merge(bb_frame, on='ASIN', how='left').fillna('').merge(mfn_frame, on='SKU',
              how='left').merge(retail_frame, on='ASIN', how='left').merge(po_frame, on='ASIN', how='left')


    def imm_la_time_predict(self, combo_frame):
        '''
        计算月平均LA剩余库存时间
        single_sku 为单品sku
        combo_frame 为包含此单品的combo与inventory数据联结的表格
        '''
        combo_frame['PrimeTime'] = combo_frame['PrimeTime'].replace(np.inf, 0).replace(np.nan, 0)
        combo_frame['WBR_Retail'] *= combo_frame['weight']
        combo_frame['WBR_LA'] *= combo_frame['weight']
        prime_time_stage = sorted(list(set(combo_frame['PrimeTime'])), reverse=False)
        la_inv = float(combo_frame.loc[combo_frame['SKU'] == combo_frame['Single'], 'MFN'])
        inv_list = [la_inv, ]
        for i, prime_time in enumerate(prime_time_stage):
            # 剩余期望库存
            inv_left = la_inv - (combo_frame['WBR_LA'] * prime_time).sum()  # LA消耗
            inv_left -= ((prime_time - combo_frame.loc[combo_frame['PrimeTime'] <= prime_time, 'PrimeTime']) *
                         combo_frame.loc[combo_frame['PrimeTime'] <= prime_time, 'WBR_Retail']).sum()  # Prime补充
            inv_list.append(inv_left)
            if inv_left < 0:
                inv_left = inv_list[-2]  # 最近的未售完库存
                wbr_total = combo_frame['WBR_LA'].sum() + combo_frame.loc[
                    combo_frame['PrimeTime'] < prime_time, 'WBR_Retail'].sum()  # 库存将为0时的WBR之和
                pre_time = prime_time_stage[i - 1] + inv_left / wbr_total  # 预期销售时间
                return pre_time
        else:
            inv_left = inv_list[-1]
            wbr_total = (combo_frame['WBR_Retail'] + combo_frame['WBR_LA']).sum()
            return prime_time_stage[-1] + inv_left / wbr_total


    def la_time_predict(self, combo_frame):
        '''
        计算即时剩余库存时间
        combo_frame 为包含此单品的combo与inventory数据联结的表格
        '''
        for column in ['SKU', 'Single', 'weight', 'MFN', 'PrimeTime', 'WBR_Retail', 'wbr']:
            if not column in combo_frame.columns: raise KeyError(f'缺少指定列 ：{column}')
        combo_frame['PrimeTime'] = combo_frame['PrimeTime'].replace(np.inf, 0).replace(np.nan, 0)
        la_inv = float(combo_frame.loc[combo_frame['SKU'] == combo_frame['Single'], 'MFN'])
        inv_list = [la_inv, ]

        prime_time_stage = sorted(list(set(combo_frame['PrimeTime'])), reverse=False)
        combo_frame['wbr_delta'] = (combo_frame['wbr'] - combo_frame['WBR_Retail']) * combo_frame[
            'weight']  # 差值为各自对应的wbr差值
        combo_frame.loc[combo_frame['wbr_delta'] < 0, 'wbr_delta'] = 0  # 负值填充为0
        combo_frame['WBR_Retail'] *= combo_frame['weight']  # 按比例放大

        for i, prime_time in enumerate(prime_time_stage):
            # 剩余期望库存
            inv_left = la_inv - (combo_frame['wbr_delta'] * prime_time).sum()  # LA消耗
            inv_left -= ((prime_time - combo_frame.loc[combo_frame['PrimeTime'] <= prime_time, 'PrimeTime']) *
                         combo_frame.loc[combo_frame['PrimeTime'] <= prime_time, 'WBR_Retail']).sum()  # Prime补充
            inv_list.append(inv_left)

            if inv_left < 0:
                inv_left = inv_list[-2]  # 最近的未售完库存
                wbr_total = combo_frame['wbr_delta'].sum() + combo_frame.loc[
                    combo_frame['PrimeTime'] < prime_time, 'WBR_Retail'].sum()  # 库存将为0时的WBR之和
                pre_time = prime_time_stage[i - 1] + np.round((inv_left / wbr_total), decimals=2)  # 预期销售时间
                return pre_time

        else:  # 库存支撑到Prime售罄
            inv_left = inv_list[-1]
            wbr_total = (combo_frame['wbr'] * combo_frame['weight']).sum()
            return prime_time_stage[-1] + (inv_left / wbr_total)


    def low_time_product_select(self):
        '''选出计算结果中剩余库存时间低于指定时间的产品信息'''
        single_frame = pd.DataFrame(
            pd.read_excel(
                self.combo_file,
                sheet_name='库存独立产品'))
        single_sku_list = single_frame['SKU'].tolist()  # 库存独立产品sku列表
        warning_item = self.total_inv_frame[
            self.total_inv_frame['SKU'].apply(lambda x: True if x in single_sku_list else False)]
        # 选出wbr_total 大于20的行
        # warning_item = warning_item[warning_item['WBR_Total'].astype('float64') > 20]
        warning_item = warning_item[(warning_item['LA库存时间'].astype('float64') < 22)]  # 选出结果中单品库存时间小于10的行
        warning_item.reset_index(drop=True, inplace=True)
        # 删除combo

        result_frame = warning_item.copy()

        result_frame.to_excel(os.path.join(self.file_settings['result_path'],'warning_results.xls'),
            index=False)  # 存为文件
        self.color_execl(os.path.join(self.file_settings['result_path'],'warning_results.xls'))


    def color_execl(self,file_path):
        '''
        file_path : 要着色的excel路径
        combo_relationship_file : 关系表，读其中single关系所在sheet
        '''
        yellosw_style = xlwt.easyxf(
            'pattern: pattern solid, fore_colour light_yellow;')  # 黄色

        rb = xlrd.open_workbook(file_path)  # 打开文件
        ro = rb.sheets()[0]  # 读取表单0
        wb = copy(rb)  # 利用xlutils.copy下的copy函数复制
        ws = wb.get_sheet(0)  # 获取表单0
        la_inventory_col = 9  # 指定修改的列，LA库存列

        for line in range(1, ro.nrows):  # 循环所有的行

            weekly_result = ro.cell(line, la_inventory_col).value
            if float(weekly_result) < 10:  # 判断是否小于10
                ws.write(
                    line,
                    la_inventory_col,
                    ro.cell(
                        line,
                        la_inventory_col).value,
                    yellosw_style)  # 修改格式
        wb.save(file_path)  # 保存文件



    def wbr_compare(self):
        '''
        读取Todd产品上周WBR数据，并与本周数据进行合并，计算两周数据差值
        '''
        total_wbr_file = self.file_settings['file_path'] + '/' + self.file_settings['total_wbr']
        todd_wbr_file = self.file_settings['file_path'] + '/' + self.file_settings['todd_wbr']
        excel_file = pd.ExcelFile(todd_wbr_file)
        # 保留上周WBR数据
        last_week_data = pd.read_excel(todd_wbr_file, sheet_name=excel_file.sheet_names[-1], usecols=range(9, 15))
        # 去掉重复列名重命名标识
        last_week_data.columns = [re.search('[A-z]+', column).group() for column in last_week_data.columns]
        # 去重并重新排序
        last_week_data.drop_duplicates(keep='first', subset='ASIN', inplace=True)
        last_week_data.reset_index(drop=True, inplace=True)

        # 读取本周WBR计算数据
        wbr_frame = pd.read_excel(total_wbr_file)
        current_week_data = wbr_frame[['ASIN', 'SKU']]
        # 按顺序排列两周数据并将新增产品SKU放置到最下方
        current_week_data = pd.concat([last_week_data[['ASIN', 'SKU']], current_week_data]).drop_duplicates(
            keep='first')
        # 添加数据
        current_week_data = current_week_data.merge(wbr_frame[['ASIN', 'WBR_Retail', 'MFN', 'XQ']], how='left')
        # 列重命名
        current_week_data.rename(columns={'WBR_Retail': 'retail', 'MFN': 'mfn', 'XQ': 'xq'}, inplace=True)
        # 求和
        current_week_data['sum'] = current_week_data[['retail', 'mfn', 'xq']].sum(axis=1)
        # 结果
        result = last_week_data.copy()
        # 保留上周数据结果
        result.columns = ['上周' + column for column in result.columns]
        # 本周数据添加到result中
        for i in range(len(current_week_data)):
            for column in current_week_data.columns:
                result.loc[i, '本周' + column] = current_week_data.loc[i, column]
        # 标识辅助列
        result[['上周', '本周', '本周-上周', '_', '__']] = [np.nan] * 5
        # 两周差值计算
        result[['ASIN', 'SKU']] = result[['本周ASIN', '本周SKU']]
        result['retail'] = result['本周retail'].fillna(0) - result['上周retail'].fillna(0)
        result['mfn'] = result['本周mfn'].fillna(0) - result['上周mfn'].fillna(0)
        result['xq'] = result['本周xq'].fillna(0) - result['上周xq'].fillna(0)
        result['sum'] = result['本周sum'].fillna(0) - result['上周sum'].fillna(0)
        # 排序
        result = result[
            ['上周', '上周ASIN', '上周SKU', '上周retail', '上周mfn', '上周xq', '上周sum', '_', '本周', '本周ASIN', '本周SKU', '本周retail',
             '本周mfn', '本周xq', '本周sum', '__', '本周-上周', 'ASIN', 'SKU', 'retail', 'mfn', 'xq', 'sum']]
        # 保存数据结果
        result_file = self.file_settings['result_path'] + '/' + self.file_settings['todd_wbr_result']
        result.to_excel(result_file, index=False)
        # 修改列名为标准格式
        book = xlrd.open_workbook(result_file)
        sheet = book.sheet_by_index(0)
        rb = copy(book)
        ro = rb.get_sheet(0)
        for col_num in range(sheet.ncols):
            text = sheet.cell(0, col_num).value
            pattern = re.search(r'.+?周([A-z]+)', text)
            if pattern:
                ro.write(0, col_num, pattern.group(1))
            elif re.search(r'_', text):
                ro.write(0, col_num, '')
        rb.save(result_file)


if __name__ == '__main__':

    d = InvDataProcess()
    d.create_result_dir()
    d.inv_data_processing()
    d.wbr_compare()