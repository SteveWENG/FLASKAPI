# -*- coding: utf-8 -*-
from sqlalchemy import func,distinct
from pandas import merge

from .Stockout import Stockout
from ....utils.functions import Error
from ..common.LangMast import lang
from ..Item.ItemMast import ItemMast

class DailyConsumption(Stockout):
    type = 'DailyConsumption'

    @classmethod
    def items(cls, data):
        costCenterCode = data.get('costCenterCode', '')
        date = data.get('date', '')
        if not costCenterCode or not date:
            Error(lang('D08CA9F5-3BA5-4DE6-9FF8-8822E5ABA1FF'))  # No data

        try:
            tmp1 = cls.ItemBatchCost(costCenterCode, date)
            if tmp1.empty:
                Error(lang('D08CA9F5-3BA5-4DE6-9FF8-8822E5ABA1FF'))  # No data

            tmp = ItemMast.list(costCenterCode)
            tmp = merge(tmp1, tmp, how='left', left_on='ItemCode', right_on='ItemCode')
            tmp.fillna('', inplace=True)
            tmp.sort_values(by=['ItemCode', 'TransDate', 'Id'], inplace=True)
            tmp['startQty'] = tmp['EndQty'] - tmp['Qty']
            tmp.rename(columns={'ItemCode': 'itemCode', 'ItemName': 'itemName', 'Stock_Unit': 'uom',
                                'Qty': 'stockQty', 'ItemCost': 'itemCost'}, inplace=True)
            return tmp[['itemCode', 'itemName', 'uom', 'itemCost', 'startQty', 'stockQty']].to_dict('records')

        except Exception as e:
            raise e

    @classmethod
    def save_check(cls, data, **kw):
        # 只能一次消耗
        tmp = cls.query.filter(cls.CostCenterCode==data[0].get('costCenterCode'),
                            cls.TransDate==data[0].get('transDate'),
                            cls.BusinessType==cls.type)\
            .with_entities(func.count(distinct(cls.TransGuid))).first()
        if tmp[0] > 1:
            Error(lang('7F6D4A6B-8F9B-425E-82CE-5E4D6FC8A147')
                  %(data[0].get('costCenterCode'),data[0].get('transDate')))

        return super().save_check(data, **kw)