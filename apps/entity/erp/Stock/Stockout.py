# -*- coding: utf-8 -*-

import pandas as pd
from pandas import merge
from sqlalchemy import or_, and_

from ..common.LangMast import lang
from ....utils.functions import *
from ..Stock import TransData

class Stockout(TransData):
    type = 'Stockout'

    def SaveData(self, *trans, **kw):
        # 服务类产品
        if not trans[0]:
            return trans[1]

        itemcosts = self.FIFO(kw.get('costCenterCode'), kw.get('transDate'), self._StockItems)
        if self._StockItems != None and itemcosts.empty:
            Error(lang('2CF04A40-C498-406F-964E-36C0B17EC765')) # No stock

        stockout = pd.DataFrame(trans[0])  # data.get('data'))

        li = merge(stockout, itemcosts, left_on='itemCode', right_on='ItemCode')
        items = ','.join(list(set(itemcosts.groupby('ItemCode').groups.keys())
                         .difference(set(li[li.EndQty >= li.qty].groupby('ItemCode').groups.keys()))))
        if items != '':
            Error(lang('8145E10D-4F00-4FEC-A1A1-B7005DC5F1B1') % items) # Shortage of some stock

        # FIFO
        li = li[li.StartQty < li.qty]
        # 分摊出库数量
        li['qty'] = li.apply(lambda l: l['StartQty'] - min(l['qty'], l['EndQty']), axis=1)

        li = li.drop(['itemCost', 'ItemCode', 'InQty', 'StartQty', 'EndQty'], axis=1) \
            .rename(columns={'ItemCost': 'itemCost'})

        self._StockItems = li.groupby('itemCode', as_index=False).agg({'INDATE': max}).to_dict('records')
        self._MAXTRANSID = itemcosts.loc[0, 'MAXID']

        return self.ZipStockList(li).to_dict('records')

    def save_check(self, data):  # Stock-item 出库
        try:

            stockout = list(filter(lambda x: x.get('isServiceItem',False)==False,data))

                # FIFO之后有新入库
            infilters = [and_(TransData.ItemCode == l.get('itemCode'),
                              TransData.TransDate <= l.get('INDATE'))
                         for l in self._StockItems if l.get('INDATE') != None]

            # 检查之后的出库
            tmp = TransData.query.filter(TransData.CostCenterCode == stockout[0].get('costCenterCode'),
                                   TransData.ItemCode.in_([l.get('itemCode') for l in self._StockItems]),
                                   TransData.TransGuid != stockout[0].get('transGuid'),
                                   or_(and_(TransData.Qty < 0, or_(TransData.TransDate > stockout[0].get('transDate'),  # 有以后的出库
                                                             TransData.Id > getInt(self._MAXTRANSID))),  # FIFO之后有出库
                                       and_(TransData.Qty > 0, TransData.Id > getInt(self._MAXTRANSID),
                                            or_(*infilters)))).first()
            if tmp == None:
                return lang('130E74A9-0A1B-4000-A82D-96A1CB13BD68') # Sucessfully saved stockout

            serr = ''
            if tmp.Qty > 0:
                # its NEW stockin prior to ."
                serr = lang('D228B8A3-D8C5-4E62-8990-EEF20EF6B387') % (tmp.ItemCode,stockout[0].get('transDate'))
            else:
                if tmp.TransDate > getDate(stockout[0].get('transDate')):
                    #  some stockout after
                    serr = lang('6D55B92C-CEA7-42FD-ABAB-86691339CBD0') % (tmp.ItemCode,stockout[0].get('transDate'))
                else:
                    # its new stockout has been added.
                    serr = lang('058B1920-1AF5-4332-B629-C8F1A2012F4D') % tmp.ItemCode

            if serr:
                Error(serr)
        except Exception as e:
            raise e
