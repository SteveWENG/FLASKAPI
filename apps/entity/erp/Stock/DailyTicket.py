# -*- coding: utf-8 -*-

from ..Order.CONTRACT import CONTRACT
from ..common.LangMast import lang
from ....utils.functions import *
from .Stockout import Stockout

class DailyTicket(Stockout):
    type = 'DailyTicket'

    @classmethod
    def items(cls, data):
        costCenterCode = data.get('costCenterCode', '')
        date = data.get('date', '')
        if not costCenterCode or not date:
            Error(lang('D08CA9F5-3BA5-4DE6-9FF8-8822E5ABA1FF'))  # No data

        df = CONTRACT.list(costCenterCode,date)
        df['LineNum'] = df['LineNum'].apply(str)
        df.rename(columns={'LineNum':'orderLineGuid','UnitPrice':'itemPrice','Unit':'uom',
                           'ItemCode':'itemCode','ItemName':'itemName'},inplace=True)

        return [{**d,'isServiceItem': True} for d in getdict(df)]

    @classmethod
    def SaveData(cls, trans, **kw):
        trans = trans[trans['qty']>0]
        trans['qty'] = -trans['qty']
        return trans #tmp.to_dict('records')

    @classmethod
    def save_check(cls, data, **kw):
        cls.CheckOrderLine(data,**kw)
        return lang('7B578615-3038-4691-BD5E-5093C62E36E4') # Successfully saved daily tickets
