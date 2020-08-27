# -*- coding: utf-8 -*-

import pandas as pd
from pandas import merge
from sqlalchemy import or_, and_

from ..common.LangMast import LangMast
from ....utils.functions import *
from ..Stock import TransData

class Stockout(TransData):
    type = 'Stockout'

    @classmethod
    def SaveData(cls, *trans, **kw):
        # 服务类产品
        if not trans[0]:
            return trans[1]

        itemcosts = cls.FIFO(kw.get('costCenterCode'), kw.get('transDate'), cls._StockItems)
        if cls._StockItems != None and itemcosts.empty:
            Error(LangMast.getText('2CF04A40-C498-406F-964E-36C0B17EC765')) # No stock

        stockout = pd.DataFrame(trans[0])  # data.get('data'))

        li = merge(stockout, itemcosts, left_on='itemCode', right_on='ItemCode')
        items = ','.join(list(set(itemcosts.groupby('ItemCode').groups.keys())
                         .difference(set(li[li.EndQty >= li.qty].groupby('ItemCode').groups.keys()))))
        if items != '':
            Error(LangMast.getText('8145E10D-4F00-4FEC-A1A1-B7005DC5F1B1') % items) # Shortage of some stock

        # FIFO
        li = li[li.StartQty < li.qty]
        # 分摊出库数量
        li['qty'] = li.apply(lambda l: l['StartQty'] - min(l['qty'], l['EndQty']), axis=1)

        li = li.drop(['itemCost', 'ItemCode', 'InQty', 'StartQty', 'EndQty'], axis=1) \
            .rename(columns={'ItemCost': 'itemCost'})

        cls._StockItems = li.groupby('itemCode', as_index=False).agg({'INDATE': max}).to_dict('records')
        cls._MAXTRANSID = itemcosts.loc[0, 'MAXID']

        return cls.ZipStockList(li).to_dict('records')

    @classmethod
    def save_check(cls, data):  # Stock-item 出库
        try:
            stockout = list(filter(lambda x: x.get('isServiceItem',False)==False,data))

                # FIFO之后有新入库
            infilters = [and_(cls.ItemCode == l.get('itemCode'),
                              cls.TransDate <= l.get('INDATE'))
                         for l in cls._StockItems if l.get('INDATE') != None]

            # 检查之后的出库
            tmp = cls.query.filter(cls.CostCenterCode == stockout[0].get('costCenterCode'),
                                   cls.ItemCode.in_([l.get('itemCode') for l in cls._StockItems]),
                                   cls.TransGuid != stockout[0].get('transGuid'),
                                   or_(and_(cls.Qty < 0, or_(cls.TransDate > stockout[0].get('transDate'),  # 有以后的出库
                                                             cls.Id > getInt(cls._MAXTRANSID))),  # FIFO之后有出库
                                       and_(cls.Qty > 0, cls.Id > getInt(cls._MAXTRANSID), or_(*infilters)))).first()
            if tmp == None:
                return LangMast.getText('130E74A9-0A1B-4000-A82D-96A1CB13BD68') # Sucessfully saved stockout

            serr = ''
            if tmp.Qty > 0:
                # its NEW stockin prior to ."
                serr = LangMast.getText('D228B8A3-D8C5-4E62-8990-EEF20EF6B387') % (tmp.ItemCode,stockout[0].get('transDate'))
            else:
                if tmp.TransDate > stockout[0].get('transDate'):
                    #  some stockout after
                    serr = LangMast.getText('6D55B92C-CEA7-42FD-ABAB-86691339CBD0') % (tmp.ItemCode,stockout[0].get('transDate'))
                else:
                    # its new stockout has been added.
                    serr = LangMast.getText('058B1920-1AF5-4332-B629-C8F1A2012F4D') % tmp.ItemCode

            if serr:
                Error(serr)
        except Exception as e:
            raise e
