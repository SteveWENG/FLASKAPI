# -*- coding: utf-8 -*-
from sqlalchemy import and_, or_, func
import pandas as pd

from ..common.LangMast import lang
from ....utils.functions import *
from ..Stock import TransData

class Stockin(TransData):
    type = 'Stockin'

    def save_check(self, data):
        try:
            if not self._MAXTRANSID:
                Error(lang('7F6D4A6B-8F9B-425E-82CE-5E4D6FC8A147')) # Error in check data after saved

            if TransData.query.filter(TransData.CostCenterCode == data[0].get('costCenterCode'),
                                TransData.ItemCode.in_(self._StockItems),
                                or_(TransData.Qty < 0,                                           # 出库
                                    and_(TransData.Qty > 0,                                      # 入库
                                         or_(TransData.TransDate < getDate(data[0].get('transDate')),        # 之前的入库
                                             and_(TransData.TransDate == getDate(data[0].get('transDate')),  # 同一天的入库
                                                  TransData.Id < self._MAXTRANSID))))) \
                    .with_entities(TransData.ItemCode).group_by(TransData.ItemCode) \
                    .having(func.sum(TransData.Qty) < 0).first() != None:
                # Can't save PO receipt, because of some stockout after"
                Error(lang('DD78E099-DF9D-49CD-95FA-C5C882D281B1') % data[0].get('transDate'))

            return lang('AAD1B983-1252-46F5-8136-9CADC200822E') # Successfully save stockin
        except Exception as e:
            raise e