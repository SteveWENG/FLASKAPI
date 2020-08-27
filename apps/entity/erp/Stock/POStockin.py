# -*- coding: utf-8 -*-

import pandas as pd

from ..Order.OrderLine import OrderLine
from ....utils.functions import Error
from .Stockin import Stockin
from ..common.LangMast import LangMast


class POStockin(Stockin):
    type = 'POReceipt'

    @classmethod
    def SaveData(cls, *trans, **kwargs):
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
    @classmethod
    def save_check(cls, data):
        try:
            super().save_check(data)

            if not data or len(data) == 0:
                Error(LangMast.getText('0CD4331A-BCD2-468A-A18A-EE4EDA2FF0EE')) # No PO lines to save

            orderLineGuids = cls.CheckOrderLine(data)

            if OrderLine.query.filter(OrderLine.Guid.in_(orderLineGuids), OrderLine.RemainQty != 0,
                                      OrderLine.DeleteTime == None) \
                .update({'RemainQty':0 },synchronize_session=False) != len(orderLineGuids) :
                Error(LangMast.getText('5B953DA5-DBD8-4301-88FB-AC94886060A7')) # This PO has been received

            return LangMast.getText('2411C461-1F80-4CAF-B142-6A44A80BFD73') # Successfully save PO receipt
        except Exception as e:
            raise e