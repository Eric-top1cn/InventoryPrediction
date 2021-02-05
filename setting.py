# -*- coding: utf-8-sig -*-
"""
File Name ：    setting
Author :        Eric
Create date ：  2020/11/9

记录文件名及字段设置等信息
"""
import re
import os
class Setting:
    def __init__(self):
        self._origin_path = input('输入文件路径:') + '/'  #r'C:\Users\Administrator\data\运营\数据运营表数据12.15日' + '/'
        self.file_settings = {
            'file_path': self._origin_path,
            'result_path':self._origin_path + 'result/',# 运行结果输出的文件夹
            'merchand_files_list': [
                '2019 Merchandising Todd-Yantai team handover from Jason.xlsx',
                '1.1 Simplify Todd _Yantai team merchandising.xlsx',
                'Yafu merchandising.xlsx',
            ],
            'todd_wbr':'WBR Todd since 18-3-26.xlsx',
            'todd_wbr_result':'todd_wbr_result.xls',

            # wbr 数据
            'retail_wbr': 'wbr_retail.xlsx',
            'retial_xq_wbr': 'wbr_retail_xq.csv',
            'mfn_wbr': 'wbr_fba&mfn.csv',
            'total_wbr':'wbr_total.xlsx',

            # inv 数据
            'bb_inv': 'BB.csv',
            'mfn_inv': 'InvNum.json',
            # 'mfn_inv': 'MFN.xlsx',
            'retial_po_inv': 'retail&po.xlsx',
            'combo_inv': 'Combo对应关系.xlsx'

        }


        self.sheet_settings = {
            'wbr_sheet': 'WBR细节',
            'combo_reflection_sheet': 'BOP导出的combo对应关系',
            'independant_sku_sheet': '库存独立产品',
            'inv_sheet_pattern': re.compile(r'\d+\-'),
            'inv_spare_sheet_pattern': re.compile(r'^inventory$|^Grow bag combo$') #['inventory','Grow bag combo'] # 备选的运营表sheet
        }


        bb_asin = 'ASIN'
        self.bb_column_settings = {
            'bb_inv_columns': ['(Child) ASIN', 'Buy Box Percentage'],
            'bb_inv_column_rename': {'(Child) ASIN': bb_asin, 'Buy Box Percentage': 'BB%'},
            'bb_aggre_column': [bb_asin, ],
        }

        mfn = 'MFN'
        mfn_sku = 'SKU'
        self.mfn_column_settings = {
            'mfn_inv_columns': ['ItemNum', 'AvailableQty'],
            'mfn_inv_column_rename': {'ItemNum': mfn_sku, 'AvailableQty': mfn},
            'mfn_index_column': mfn_sku,
            'mfn_zero_fill_column': [mfn,],  # 将指定列小于0的值修改为0
        }

        retail_asin = 'ASIN'
        po_asin = 'ASIN'

        self.retail_column_settings = {
            'retail_inv_columns': [retail_asin, 'Sellable On Hand Units'],
            'retail_inv_column_rename': {'Sellable On Hand Units': 'retail'},
            'retail_index_column': retail_asin,

            'po_inv_columns': [po_asin, 'Open Purchase Order Quantity'],
            'po_inv_column_rename': {'Open Purchase Order Quantity': 'open_po'},
            'po_index_column': po_asin,
        }

        self.combo_column_settings = {
            'combo_relation_column': ['ItemNum', 'ChildItemNum', 'ChildQty'],
            'combo_relation_column_rename': {'ItemNum': 'Combo', 'ChildItemNum': 'Single', 'ChildQty': 'weight'},
        }

        inv_prdt_line = 'ProductLine'
        inv_asin = 'ASIN'
        self.inv_column_settings = {
            'inv_frame_column': [inv_prdt_line, inv_asin, 'SKU'],  # 保留的运营表列
            'frame_filled': {'need_filled': True, 'columns': [inv_prdt_line,], 'fill_method': 'ffill', },  # 指定要填充的列及填充方式
            'inv_asin_filter_conditon': {'filter_column': inv_asin, 'filter_condition': re.compile(r'^B')},
            # inv_concat表保留有效行的筛选条件
            'inv_concat': 'total',  # 运营表数据合并后的dataframe 在记录运营表数据字典中的key
            # total inv frame 合并后要转换成float类型的列名
            'converse_float_columns': ['MFN', 'retail','WBR_Retail']
        }

        self.wbr_column_settings = {

            'retail_columns': ['ASIN', 'Shipped Units'],  # retail  要保留的字段
            'retail_columns_rename': {'Shipped Units': 'WBR_Retail'},  # retail 重命名字段

            'retail_xq_columns': ['ASIN', 'Item Quantity'],  # xq 要保留的字段
            'retail_xq_columns_rename': {'Item Quantity': 'XQ'},  # xq 重命名字段
            'retail_xq_aggre_column': ['ASIN', ],  # 求和字段

            'mfn_channel_column_name': 'sales-channel',  # mfn 要筛选的channel 列名
            'mfn_channel_column_value': ['Amazon.com'],  # mfn 保留的channel列值
            'mfn_status_column_name': 'order-status',  # mfn 要筛选的status 列名
            'mfn_status_column_value': ['Cancelled', ],  # mfn 要过滤掉的状态列表
            'mfn_fulfillment_column_name': 'fulfillment-channel',  # mfn 要筛选的 配送方式 列名
            'mfn_fulfillment_column_value': ['Amazon', ],  # mfn 要过滤掉的配送方式列表
            'mfn_columns': ['asin', 'quantity'],  # mfn 保留的列
            'mfn_columns_rename': {'asin': 'ASIN', 'quantity': 'MFN'},  # mfn 列重命名
            'mfn_aggre_column': ['ASIN', ],  # mfn 求和字段

            'wbr_dropped_columns': ['WBR_Retail', 'MFN', 'WBR_LA', 'XQ'], # 将wbr表格写入文件时要删除的字段
            'wbr_la_column': 'WBR_LA',  # wbr 表格中记录当前周LA销量的列名
            'wbr_la_detail_columns': ['MFN',],  # 'XQ'],  # 组成wbr_la销量的多行字段名，用于求和
            'wbr_total_columns': ['WBR_LA', 'WBR_Retail'],  # 计算wbr总值对应的列名
            'wbr_last_week_column': 'd4',  # 记录上周wbr和值的列
            'wbr_weekly_avg_column': 'wbr',  # 记录上月wbr均值所在的列名
            'wbr_weekly_avg_detail_columns': ['d1', 'd2', 'd3', 'd4']
        }

        self.data_processing_setting = {
            'wbr_merge_column': 'ASIN',  # wbr 与 inv join 选择的合并列, left连接
            'wbr_effective_columns': ['wbr','WBR_Retail', 'WBR_LA'],  # 合并后保留的wbr列
            'pt_column':'PrimeTime', # primetime列
            # 'pt_numerator_columns':['retail'], #,'open_po'], # 计算primetime的分子列求和 retial open_po
            'pt_numerator_columns':['retail','open_po'], # 计算primetime的分子列求和 retial open_po
            'pr_denominator_columns':['WBR_Retail'] # 计算primetime的分母列求和
        }

        self.low_inv_filter_setting = {

        }

    def create_result_dir(self):
        if os.path.exists(self.file_settings['result_path']):return
        os.mkdir(self.file_settings['result_path'])
