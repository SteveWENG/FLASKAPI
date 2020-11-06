# -*- coding: utf-8 -*-

from sqlalchemy import or_,and_,func,case
import pandas as pd
from pandas import merge

from .Stockout import Stockout
from ..Order.OrderHead import OrderHead
from ..Order.OrderLine import OrderLine
from ..Item.ItemMast import ItemMast
from ..common.CCMast import CCMast
from ....utils.functions import *

class POReturn(Stockout):
    type = 'POReturn'
    msgSaveOk = '6D4952CA-CFC4-4D67-A02F-D65308761665'

    @classmethod
    def items(cls, data):
        costCenterCode = data.get('costCenterCode', '')
        date = data.get('date', '')
        orderNo = data.get('orderNo','')
        if not costCenterCode or not date or not orderNo:
            Error(lang('D08CA9F5-3BA5-4DE6-9FF8-8822E5ABA1FF'))  # No data

        try:
            # 采购入库
            qryIn = cls.query.join(OrderLine, cls.OrderLineGuid==OrderLine.Guid)\
                .join(OrderHead, OrderLine.HeadGuid==OrderHead.HeadGuid)\
                .join(CCMast, CCMast.CostCenterCode==costCenterCode) \
                .join(ItemMast, and_(cls.ItemCode == ItemMast.ItemCode, CCMast.DBName == ItemMast.Division))\
                .filter(cls.CostCenterCode==costCenterCode,cls.BusinessType=='POReceipt',
                        OrderHead.OrderDate==date, OrderHead.OrderNo==orderNo, cls.Qty>1/1000000)\
                .with_entities(cls.SysGuid,cls.BatchGuid.label('batchGuid'),cls.ItemCode.label('itemCode'),
                               cls.ItemName.label('itemName'),
                               OrderLine.Qty.label('purQty'),OrderLine.PurchaseUnit.label('purUnit'),
                               OrderLine.PurchasePrice.label('purPrice'),
                               OrderLine.PurStk_Conversion.label('purStk_Conversion'), cls.Qty)
            qryIn = pd.read_sql(qryIn.statement,cls.getBind())

            # 除采购入库
            qryOut = cls.query.filter(cls.BatchGuid.in_(set(qryIn['batchGuid'].tolist())),
                                      cls.BusinessType != 'POReceipt')\
                .with_entities(cls.SysGuid,cls.BatchGuid.label('batchGuid'),func.min(cls.TransDate).label('outDate'),
                               func.sum(cls.Qty).label('outQty'))\
                .group_by(cls.SysGuid,cls.BatchGuid)\
                .having(func.sum(cls.Qty)<-1/1000000)
            qryOut = pd.read_sql(qryOut.statement,cls.getBind())

            # 合并采购入库和退货
            qryIn = merge(qryIn,qryOut[['SysGuid','outQty']],how='left',left_on='SysGuid',right_on='SysGuid')
            qryIn.loc[~qryIn['outQty'].isna(),'Qty'] = qryIn['Qty'] + qryIn['outQty']
            qryIn = qryIn[qryIn['Qty']>0]
            qryIn['Qty'] = qryIn['Qty'] / qryIn['purStk_Conversion']
            qryIn.rename(columns={'Qty': 'inQty'},inplace=True)
            sysguids = set(qryIn['SysGuid'].tolist())
            qryIn.drop(['outQty','SysGuid'], axis=1,inplace=True)

            qryIn = merge(qryIn, qryOut[~qryOut['SysGuid'].isin(sysguids)],
                          how='left',left_on='batchGuid',right_on='batchGuid')

            qryIn.loc[~qryIn['outQty'].isna(),'outQty'] = qryIn['outQty'] / qryIn['purStk_Conversion']
            qryIn.loc[qryIn['outDate']==qryIn['outDate'],'outDate'] = \
                qryIn.loc[qryIn['outDate']==qryIn['outDate'],'outDate'].map(lambda x: x.strftime('%Y-%m-%d'))
            qryIn.fillna('',inplace=True)
            return qryIn.drop('SysGuid',axis=1)\
                .sort_values(by=['itemCode','batchGuid','outDate'],ascending=[1,1,0])\
                .to_dict('records')
        except Exception as e:
            raise e

    @classmethod
    def SaveData(cls, trans, **kw):
        # 相同批号的采购入库和其它
        stocktrans = cls.query.filter(cls.BatchGuid.in_(set(trans['batchGuid'].tolist())),
                                      cls.CostCenterCode==kw.get('costCenterCode'))\
            .with_entities(func.min(cls.Id).label('Id'),cls.SysGuid,cls.BatchGuid,
                           func.min(cls.TransDate).label('TransDate'),
                           cls.ItemCost, func.sum(cls.Qty).label('TransQty'),
                           func.max(case([(cls.BusinessType=='POReturn','')],else_=cls.BusinessType)).label('BusinessType'))\
            .group_by(cls.SysGuid,cls.BatchGuid,cls.ItemCost)
        stocktrans = pd.read_sql(stocktrans.statement,cls.getBind())

        # 采购退货数量
        trans['qty'] = -1 * trans['purQty'] * trans['purStk_Conversion']
        qry = merge(trans,stocktrans[stocktrans['BusinessType']=='POReceipt'],
                    how='left', left_on='batchGuid', right_on='BatchGuid')

        # 退货<=入库，
        tmp = ''.join(set(qry.loc[(qry['TransQty'].isna())|(qry['qty']+qry['TransQty'] < 0), 'itemCode']))
        if tmp:
            Error('退货大于入库(%s)' % tmp)

        # 采购退货记录
        rslt = qry[['SysGuid','BatchGuid','TransDate','itemCode','itemName','ItemCost','qty']]

        # 检查库存是否满足退货，否则需要出库退货
        stocktrans.sort_values(by=['BatchGuid', 'TransDate', 'Id'], inplace=True)
        stocktrans['EndQty'] = stocktrans.groupby('BatchGuid')['TransQty'].cumsum()
        qry = merge(trans, stocktrans[stocktrans['BusinessType'] !='POReceipt'],
                    left_on='batchGuid', right_on='BatchGuid')

        if not qry.empty: #有出库记录
            qry = qry[qry['qty']+qry['EndQty']<0]

            if not qry.empty: # 库存 < 退货
                qry['StartQty'] = qry['EndQty'] - qry['TransQty']
                # 库存不满足采购退货
                qry['OutQty'] = qry.apply(lambda x: -1 * max(x['TransQty'],x['EndQty']+x['qty']),axis=1)

                rslt = qry[['SysGuid','BatchGuid','TransDate','itemCode','itemName','ItemCost','OutQty']]\
                    .rename(columns={'OutQty':'qty'}).append(rslt,ignore_index=True)

        # 采购退货日期 = max(出库日期)
        rslt['TransDate'] = rslt.apply(lambda x: x['TransDate'] if qry[qry['BatchGuid'] == x['BatchGuid']].empty
            else qry.loc[qry['BatchGuid'] == x['BatchGuid'], 'TransDate'].max(), axis=1)

        return rslt
