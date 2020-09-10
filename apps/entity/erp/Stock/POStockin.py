# -*- coding: utf-8 -*-

import pandas as pd
from sqlalchemy import func, distinct

from ..Order.OrderLineF import OrderLineF
from ....utils.functions import *
from .Stockin import Stockin
from ..common.LangMast import lang


class POStockin(Stockin):
    type = 'POReceipt'

    @classmethod
    def SaveData(cls, trans, **kwargs):

        # Purchase UOM => Stock UOM
        # stockin = pd.DataFrame(trans)

        #if not trans[(trans['purQty']>0)&(trans['purStk_Conversion']==0)].empty:
        #    Error(lang('3BD6363C-F4BD-46A8-A788-2CD6031E468E'))

        trans.loc[trans['purQty']>0,'qty'] = trans['purQty'] * trans['purStk_Conversion']
        trans.loc[trans['purQty']>0,'itemCost'] = trans['purPrice'] / trans['purStk_Conversion']
        trans.drop(['purQty', 'purPrice', 'purStk_Conversion'], axis=1, inplace=True)
        # 每次入库唯一号码
        trans['guid'] = [getGUID() for x in range(len(trans))]
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
            guids = set([s.get('orderLineGUID') for s in data])

            if OrderLineF.query.filter(OrderLineF.Guid.in_(guids), OrderLineF.RemainQty != 0,
                                      func.lower(OrderLineF.Status)=='created',OrderLineF.DeleteTime == None) \
                .update({'RemainQty':0 },synchronize_session=False) < len(data) :
                Error(lang('5B953DA5-DBD8-4301-88FB-AC94886060A7')) # This PO has been received

            return lang('2411C461-1F80-4CAF-B142-6A44A80BFD73') # Successfully save PO receipt
        except Exception as e:
            raise e
