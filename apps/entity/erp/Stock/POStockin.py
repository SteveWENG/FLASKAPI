# -*- coding: utf-8 -*-

from pandas import merge
import pandas as pd
from sqlalchemy import func

from ..Item.ItemMast import ItemMast
from ..Order.OrderHead import OrderHead
from ..Order.OrderLine import OrderLine
from ....utils.functions import *
from .Stockin import Stockin
from ..common.LangMast import lang


class POStockin(Stockin):
    type = 'POReceipt'

    @classmethod
    def items(cls, data):
        costCenterCode = data.get('costCenterCode', '')
        date = data.get('date', '')
        supplierCode = data.get('supplierCode','')
        if not costCenterCode or not date or not supplierCode:
            Error(lang('D08CA9F5-3BA5-4DE6-9FF8-8822E5ABA1FF'))  # No data

        tmp1 = OrderHead.list(costCenterCode,date,supplierCode)
        tmp2 = ItemMast.list(costCenterCode)[['ItemCode','ItemName','Stock_Unit']]
        tmp1 = merge(tmp1,tmp2,left_on='itemCode',right_on='ItemCode')
        tmp1.rename(columns={'ItemName':'itemName','Stock_Unit':'uom','CreateTime': 'orderLineCreateTime'}, inplace=True)
        return tmp1.to_dict('records')

    @classmethod
    def SaveData(cls, trans, **kwargs):

        # Purchase UOM => Stock UOM
        # stockin = pd.DataFrame(trans)

        #if not trans[(trans['purQty']>0)&(trans['purStk_Conversion']==0)].empty:
        #    Error(lang('3BD6363C-F4BD-46A8-A788-2CD6031E468E'))

        trans = trans[trans['purQty']>0]
        if not trans[trans['purStk_Conversion']==0].empty:
           Error(lang('3BD6363C-F4BD-46A8-A788-2CD6031E468E'))

        trans['qty'] = trans['purQty'] * trans['purStk_Conversion']
        trans['itemCost'] = trans['purPrice'] / trans['purStk_Conversion']
        trans.drop(['purQty', 'purPrice', 'purStk_Conversion'], axis=1, inplace=True)

        if kwargs.get('supplierCode','') != '':
            trans['supplierCode'] = kwargs.get('supplierCode','')

        return trans# stockin.to_dict('records')

    #1 检查对应的PO是否已入库
    #2 更新POlines中的剩余数量=0
    @classmethod
    def save_check(cls, data, **kw):
        try:
            if not data or len(data) == 0:
                Error(lang('D08CA9F5-3BA5-4DE6-9FF8-8822E5ABA1FF')) # No PO lines to save

            cls.CheckOrderLine(data)
            guids = set([s.get('orderLineGuid') for s in data])

            createTime = getDateTime(kw.get('orderLineCreateTime')) + datetime.timedelta(seconds=1)
            if OrderLine.query.filter(OrderLine.Guid.in_(guids), OrderLine.RemainQty != 0,
                                      OrderLine.CreateTime<createTime,
                                      func.lower(OrderLine.Status)=='created',OrderLine.DeleteTime == None) \
                .update({'RemainQty':0 },synchronize_session=False) < len(data) :
                Error(lang('5B953DA5-DBD8-4301-88FB-AC94886060A7')) # This PO has been received or changed

            return lang('2411C461-1F80-4CAF-B142-6A44A80BFD73') # Successfully save PO receipt
        except Exception as e:
            raise e
