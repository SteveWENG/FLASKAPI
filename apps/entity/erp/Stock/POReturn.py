# -*- coding: utf-8 -*-

from sqlalchemy import and_,func,case
from sqlalchemy.orm import aliased
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

    @classmethod
    def items(cls, data):
        costCenterCode = data.get('costCenterCode', '')
        date = data.get('date', '')
        orderNo = data.get('orderNo','')
        if not costCenterCode or not date or not orderNo:
            Error(lang('D08CA9F5-3BA5-4DE6-9FF8-8822E5ABA1FF'))  # No data

        try:
            stockout = aliased(cls)
            qry = cls.query.join(OrderLine, cls.OrderLineGuid==OrderLine.Guid)\
                .join(OrderHead, OrderLine.HeadGuid==OrderHead.HeadGuid)\
                .join(CCMast, CCMast.CostCenterCode==costCenterCode) \
                .join(ItemMast, and_(cls.ItemCode == ItemMast.ItemCode, CCMast.DBName == ItemMast.Division))\
                .outerjoin(stockout, and_(stockout.CostCenterCode==costCenterCode,
                                          cls.BatchGuid==stockout.BatchGuid,stockout.BusinessType != 'POReceipt'))\
                .filter(cls.CostCenterCode==costCenterCode,cls.BusinessType=='POReceipt',
                        cls.TransDate==date, OrderHead.OrderNo==orderNo,)\
                .with_entities(cls.BatchGuid.label('batchGuid'),cls.ItemCode.label('itemCode'),
                               cls.ItemName.label('itemName'),
                               func.round(cls.Qty/OrderLine.PurStk_Conversion,6).label('inQty'),
                               stockout.BatchGuid.label('outBatchGuid'),
                               func.round(func.coalesce(stockout.Qty,0)/OrderLine.PurStk_Conversion,6).label('outQty'),
                               OrderLine.Qty.label('purQty'),
                               OrderLine.PurchaseUnit.label('purUnit'),
                               OrderLine.PurchasePrice.label('purPrice'),
                               OrderLine.PurStk_Conversion.label('purStk_Conversion'),
                               stockout.TransDate.label('outDate'))\
                .order_by(cls.ItemCode,stockout.TransDate.desc(),stockout.Id.desc()).all()

            return getdict(qry)
        except Exception as e:
            raise e

    @classmethod
    def SaveData(cls, trans, **kw):
        # 相同批号的采购入库和其它
        stocktrans = cls.query.filter(cls.BatchGuid.in_(set(trans['batchGuid'].tolist())),
                                      cls.CostCenterCode==kw.get('costCenterCode'))\
            .with_entities(cls.BatchGuid,cls.TransDate,cls.ItemCost,
                           cls.Qty.label('TransQty'),cls.BusinessType)\
            .order_by(cls.BatchGuid,cls.TransDate,cls.Id)
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
        rslt = qry[['BatchGuid','TransDate','itemCode','ItemCost','qty']]

        stocktrans['EndQty'] = stocktrans.groupby('BatchGuid')['TransQty'].cumsum()
        qry = merge(trans, stocktrans[stocktrans['BusinessType'] !='POReceipt'],
                    left_on='batchGuid', right_on='BatchGuid')

        # 采购退货日期 = max(出库日期)
        rslt['TransDate'] = rslt.apply(lambda x: x['TransDate'] if qry[qry['BatchGuid'] == x['BatchGuid']].empty
            else qry.loc[qry['BatchGuid'] == x['BatchGuid'], 'TransDate'].max(), axis=1)

        qry = qry[qry['qty']+qry['EndQty']<0]
        qry['StartQty'] = qry['EndQty'] - qry['TransQty']
        # 库存不满足采购退货
        qry['OutQty'] = qry.apply(lambda x: -1 * max(x['TransQty'],x['EndQty']+x['qty']),axis=1)

        rslt = rslt.append(qry[['BatchGuid','TransDate','itemCode','ItemCost','OutQty']]
                           .rename(columns={'OutQty':'qty'}),ignore_index=True)
        return rslt
