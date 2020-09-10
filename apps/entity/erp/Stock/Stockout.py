# -*- coding: utf-8 -*-

from pandas import merge
from sqlalchemy import func


from ..common.LangMast import lang
from ....utils.functions import *
from ..Stock import TransData

class Stockout(TransData):
    type = 'Stockout'

    @classmethod
    def SaveData(cls, trans, **kw):

        itemcosts = cls.ItemBatchCost(kw.get('costCenterCode'), kw.get('transDate'), kw.get('itemCodes'))
        if kw.get('itemCodes','') != '' and itemcosts.empty:
            Error(lang('2CF04A40-C498-406F-964E-36C0B17EC765')) # No stock

        #stockout = pd.DataFrame(trans)  # data.get('data'))

        li = merge(trans, itemcosts, how='left', left_on='itemCode', right_on='ItemCode')
        for s in ['EndQty','qty']:
            li[s].fillna(0,inplace=True)
        items = ','.join(list(li[li.EndQty < li.qty].groupby('itemCode').groups.keys()))

            # list(set(itemcosts.groupby('ItemCode').groups.keys()).difference(set(li[li.EndQty >= li.qty].groupby('ItemCode').groups.keys()))))
        if items != '':
            Error(lang('8145E10D-4F00-4FEC-A1A1-B7005DC5F1B1') % items) # Shortage of some stock

        # 分摊出库数量
        li = li[(li.EndQty - li.Qty) < li.qty]
        li['qty'] = li.apply(lambda l: max(l['EndQty']-l['qty'],0)-l['Qty'], axis=1)

        li.drop(['Id', 'ItemCode','TransDate','Qty', 'EndQty'], axis=1, inplace=True)
            # .rename(columns={'ItemCost': 'itemCost'})

        return li

    @classmethod
    def save_check(cls, data, **kw):  # Stock-item 出库
        try:

            # 按批号合并，检查库存有效
            guids = set([l.get('Guid') for l in data])
            tmp = cls.query.filter(func.coalesce(cls.Guid,'').in_(guids)).with_entities(cls.ItemCode)\
                .group_by(cls.ItemCode).having(func.round(func.sum(func.coalesce(cls.Qty,0)),6) < -1/1000000).all()
            if not tmp:
                return lang('130E74A9-0A1B-4000-A82D-96A1CB13BD68') # Sucessfully saved stockout

            Error(lang('8145E10D-4F00-4FEC-A1A1-B7005DC5F1B1') % ','.join(set([l.ItemCode for l in tmp])))
        except Exception as e:
            raise e
