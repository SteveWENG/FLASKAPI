# -*- coding: utf-8 -*-

import pandas as pd
from sqlalchemy import func, distinct

from ..Order.OrderLine import OrderLine
from ....utils.functions import Error
from .Stockin import Stockin
from ..common.LangMast import lang


class POStockin(Stockin):
    type = 'POReceipt'

    def SaveData(self, *trans, **kwargs):
        # Purchase UOM => Stock UOM
        stockin = pd.DataFrame(trans[0])
        stockin.loc[stockin['purStkConversion'] != 0, 'qty'] = stockin['purQty'] * stockin['purStkConversion']
        stockin.loc[stockin['purStkConversion'] != 0, 'itemCost'] = stockin['purPrice'] / stockin['purStkConversion']
        stockin = stockin.drop(['purQty', 'purPrice', 'purStkConversion'], axis=1)

        if kwargs.get('supplierCode','') != '':
            stockin['supplierCode'] = kwargs.get('supplierCode','')

        return stockin.to_dict('records')

    #1 检查对应的PO是否已入库
    #2 更新POlines中的剩余数量=0
    def save_check(self, data):
        try:
            super().save_check(data)

            if not data or len(data) == 0:
                Error(lang('0CD4331A-BCD2-468A-A18A-EE4EDA2FF0EE')) # No PO lines to save

            self.CheckOrderLine(data)

            if OrderLine.query.filter(OrderLine.HeadGuid==data[0].get('orderGuid'), OrderLine.RemainQty != 0,
                                      OrderLine.DeleteTime == None) \
                .update({'RemainQty':0 },synchronize_session=False) < len(data) :
                Error(lang('5B953DA5-DBD8-4301-88FB-AC94886060A7')) # This PO has been received

            return lang('2411C461-1F80-4CAF-B142-6A44A80BFD73') # Successfully save PO receipt
        except Exception as e:
            raise e

    def GetOrderLine(self, lineGuids, date=None):
        clz = type(self)
        return clz.query.filter(clz.OrderLineGuid.in_(lineGuids))\
            .with_entities(func.count(distinct(clz.OrderLineGuid)).label('GuidCount'),  # 全部save
                           func.count(clz.Id).label('TotalCount')).first()  # 重复save