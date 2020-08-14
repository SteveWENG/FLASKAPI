# -*- coding: utf-8 -*-

import pandas as pd
from pandas import merge
from sqlalchemy import or_, and_

from ....utils.functions import *
from ..Stock import TransData

class Stockout(TransData):
    type = 'Stockin'

    @classmethod
    def save(cls, data):
        try:
            if data.get('data') == None or len(data.get('data')) == 0:
                Error('No data to save')

            dic, itemcodes = cls.PrepareSave(data)
            itemcosts = cls.show(dic.get('costCenterCode'), dic.get('transDate'), itemcodes)
            if itemcosts.empty:
                Error('No stock')

            stockout = pd.DataFrame(data.get('data'))

            li = merge(stockout, itemcosts, left_on='itemCode', right_on='ItemCode' )
            if len(li[li.EndQty >= li.qty].groupby('itemCode')) < len(itemcosts.groupby('ItemCode')):
                Error('Shortage of some stock')

            # FIFO
            li = li[li.StartQty < li.qty]
            # 分摊出库数量
            li['qty'] = li.apply(lambda l: l['StartQty'] - min(l['qty'],l['EndQty']),axis=1)

            li = li.drop(['Id','itemCost','ItemCode','InQty','StartQty','EndQty'], axis=1)\
                .rename(columns={'ItemCost': 'itemCost'})

            item_dates = li.groupby('itemCode', as_index=False).agg({'INDATE': max}).to_dict('records')
            MAXINID = getNumber(itemcosts.loc[0,'MAXINID'])
            MAXOUTID = getNumber(itemcosts['OUTID'].max())

            li = [dict(l, **dic) for l in cls.ZipStockList(li)]

            with cls.adds(li) as session:
                # FIFO之后有新入库
                infilters = [and_(cls.ItemCode==l.get('itemCode'),
                                  cls.TransDate<=l.get('INDATE'))
                           for l in item_dates if l.get('INDATE') != None]

                #检查之后的出库
                tmp = cls.query.filter(cls.CostCenterCode==dic.get('costCenterCode'),
                                       cls.ItemCode.in_([l.get('itemCode') for l in item_dates]),
                                       cls.TransGuid != dic.get('transGuid'),
                                       or_( and_(cls.Qty<0, or_(cls.TransDate > dic.get('transDate'), # 有以后的出库
                                                                cls.Id > MAXOUTID)),                  # FIFO之后有出库
                                            and_(cls.Qty>0, cls.Id >MAXINID, or_(*infilters)))).first()
                if tmp == None:
                    return 'Sucessfully save stockout'

                serr = "%s can't be saved because " %tmp.ItemCode
                if tmp.Qty > 0:
                    Error("%s its NEW stockin prior to %s." %(serr, dic.get('transDate')) )
                else :
                    Error("% its stockout has been changed." %serr)

        except Exception as e:
            raise e