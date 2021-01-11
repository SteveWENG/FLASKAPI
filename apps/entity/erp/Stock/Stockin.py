# -*- coding: utf-8 -*-
from sqlalchemy import  func
from sqlalchemy.orm import aliased

from ..common.LangMast import lang
from ....utils.functions import *
from ..Stock import TransData

class Stockin(TransData):
    type = 'Stockin'

    '''
    @classmethod
    def save_check(cls, data, **kw):
        try:
            clzout = aliased(cls)
            tmp = cls.query.join(clzout,cls.Guid==clzout.Guid)\
                .filter(TransData.CostCenterCode==data[0].get('costCenterCode'),
                        TransData.ItemCode.in_(kw.get('itemCodes')),
                        cls.TransDate>getDate(data[0].get('transDate')),
                        func.round(func.coalesce(cls.Qty,0),6) > 1/1000000,                # 入库
                        func.round(func.coalesce(clzout.Qty,0),6) < -1/1000000).first()    # 出库
            if tmp != None:
                # Can't save PO receipt, because of some stockin after ? has been consumed"
                Error(lang('DD78E099-DF9D-49CD-95FA-C5C882D281B1') % (tmp.ItemCode,data[0].get('transDate')))

            return lang('AAD1B983-1252-46F5-8136-9CADC200822E') # Successfully save stockin
        except Exception as e:
            raise e
    '''