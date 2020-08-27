# -*- coding: utf-8 -*-
from sqlalchemy import and_, or_, func
import pandas as pd

from ..common.LangMast import LangMast
from ....utils.functions import *
from ..Stock import TransData

class Stockin(TransData):
    type = 'Stockin'

    # StockItems, NewId
    @classmethod
    def save_check(cls, data):
        try:
            if not cls._MAXTRANSID:
                Error(LangMast.getText('7F6D4A6B-8F9B-425E-82CE-5E4D6FC8A147')) # Error in check data after saved

            if cls.query.filter(cls.CostCenterCode == data[0].get('costCenterCode'),
                                cls.ItemCode.in_(cls._StockItems),
                                or_(cls.Qty < 0,                                           # 出库
                                    and_(cls.Qty > 0,                                      # 入库
                                         or_(cls.TransDate < getDate(data[0].get('transDate')),        # 之前的入库
                                             and_(cls.TransDate == getDate(data[0].get('transDate')),  # 同一天的入库
                                                  cls.Id < cls._MAXTRANSID))))) \
                    .with_entities(cls.ItemCode).group_by(cls.ItemCode) \
                    .having(func.sum(cls.Qty) < 0).first() != None:
                # Can't save PO receipt, because of some stockout after"
                Error(LangMast.getText('DD78E099-DF9D-49CD-95FA-C5C882D281B1') % data[0].get('transDate'))

            return LangMast.getText('AAD1B983-1252-46F5-8136-9CADC200822E') # Successfully save stockin
        except Exception as e:
            raise e