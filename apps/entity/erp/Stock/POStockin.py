# -*- coding: utf-8 -*-

from pandas import merge
import pandas as pd
from sqlalchemy import func
from functools import reduce

from ..Order.OrderHead import OrderHead
from ..Order.OrderLine import OrderLine
from ....utils.functions import *
from .Stockin import Stockin
from ..common.LangMast import lang,getParameters
from ..Item import Item


class POStockin(Stockin):
    type = 'POReceipt'

    @classmethod
    def items(cls, data):
        headGuid = data.get('headGuid', '')
        costCenterCode = data.get('costCenterCode', '')
        date = data.get('date', '')
        supplierCode = data.get('supplierCode','')
        orderType = data.get('orderType','')
        if not headGuid or not costCenterCode or not date or not supplierCode or not orderType:
            Error(lang('D08CA9F5-3BA5-4DE6-9FF8-8822E5ABA1FF'))  # No data
        headGuid,costCenterCode,date,supplierCode,orderType = \
            getParameters(data,['headGuid','costCenterCode','date','supplierCode','orderType'])

        tmp1 = OrderHead.listToStock(headGuid,costCenterCode,date,supplierCode,orderType,)
        if tmp1.empty:
            Error(lang('D08CA9F5-3BA5-4DE6-9FF8-8822E5ABA1FF'))  # No data

        tmp2 = Item.list() # ItemMast.list(costCenterCode)[['ItemCode','ItemName','Stock_Unit']]
        tmp1 = merge(tmp1,tmp2,left_on='itemCode',right_on='ItemCode')
        tmp1.rename(columns={'ItemName':'itemName','Stock_Unit':'uom','CreateTime': 'orderLineCreateTime'}, inplace=True)
        # 限制最大入库数量
        tmp1['stockQty'] = tmp1['qty'] * 1.1

        return tmp1.to_dict('records')

    @classmethod
    def dates(cls, data):
        costCenterCode = data.get('costCenterCode', '')
        if not costCenterCode:
            Error(lang('D08CA9F5-3BA5-4DE6-9FF8-8822E5ABA1FF'))  # No data

        df = OrderHead.datesToStock(costCenterCode)
        if df.empty:
            Error(lang('D08CA9F5-3BA5-4DE6-9FF8-8822E5ABA1FF'))  # No data

        df.columns = [x[0].lower()+x[1:] for x in df.columns]
        df.rename(columns={'orderDate':'date'},inplace=True)
        indexFields = ['headGuid','supplierCode','orderNo','orderType']
        df['sortValue'] = df['poType'].map(lambda x: 1 if x.lower()=='normal' else (2 if x.lower()=='addition' else 3))
        df = df.sort_values(by=['sortValue']).groupby(by=['sortValue','poType','date'])\
            .apply(lambda g:pd.Series(
            {'lines':reduce(lambda x1,x2:x1+x2,
                            [[{'index':{f:l[f] for f in indexFields},
                               'label':'%s %s (%s)' %(l['supplierCode'],l['supplierName'],l['orderNo'])}]
                             for l in g.to_dict('records')])}))\
            .reset_index().drop(['sortValue'],axis=1)

        df['disabled'] = 0
        if not df[df['poType'].str.lower()=='normal'].empty:
            df.at[df[df['poType'].str.lower()=='normal'].index.min(), 'disabled']= 1

        return getdict(df)

    @classmethod
    def SaveData(cls, trans, **kwargs):

        # 只计算入库数量>0
        #trans = trans[trans['purQty']>0]

        # 必须有采购单位 -> 库存单位的系数
        # 0收货
        if ('purQty' not in trans.columns) or ('purStk_Conversion' not in trans.columns) or \
                not trans[(trans['purQty']>0)&(trans['purStk_Conversion']==0)].empty:
           return trans # Error(lang('3BD6363C-F4BD-46A8-A788-2CD6031E468E'))


        trans.loc[trans['purQty']>0,'qty'] = trans['purQty'] * trans['purStk_Conversion']
        trans.loc[trans['purQty']>0,'itemCost'] = trans['purPrice'] / trans['purStk_Conversion']
        trans.drop(['purQty', 'purPrice', 'purStk_Conversion'], axis=1, inplace=True)
        '''
        if kwargs.get('supplierCode',''):
            trans['supplierCode'] = kwargs.get('supplierCode','')
        if kwargs.get('supplierName','') != '':
            trans['supplierName'] = kwargs.get('supplierName','')
        '''
        return trans# stockin.to_dict('records')

    #1 检查对应的PO是否已入库
    #2 更新POlines中的剩余数量=0
    @classmethod
    def save_check(cls, data, **kw):
        try:
            '''0收货
            if not data or len(data) == 0:
                Error(lang('D08CA9F5-3BA5-4DE6-9FF8-8822E5ABA1FF')) # No PO lines to save
            '''

            cls.CheckOrderLine(data,**kw)
            guids = set([s.get('orderLineGuid') for s in data])

            # 更新POlines中的剩余数量=0，并检查入库的记录数与更新剩余数量的记录数是否一致
            createTime = getDateTime(kw.get('orderLineCreateTime')) + datetime.timedelta(seconds=1)
            session = kw.get('session')
            if session.query(OrderLine).filter(OrderLine.Guid.in_(guids), OrderLine.RemainQty==OrderLine.Qty,
                                      OrderLine.CreateTime<createTime,
                                      func.lower(OrderLine.Status)=='created',OrderLine.DeleteTime == None) \
                .update({'RemainQty':0 },synchronize_session=False) < len(guids) :
                Error(lang('5B953DA5-DBD8-4301-88FB-AC94886060A7')) # This PO has been received or changed

            return lang('2411C461-1F80-4CAF-B142-6A44A80BFD73') # Successfully save PO receipt
        except Exception as e:
            raise e
