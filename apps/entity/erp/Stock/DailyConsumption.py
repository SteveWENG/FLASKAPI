# -*- coding: utf-8 -*-

from .Stockout import Stockout
from ....utils.functions import Error
from ..common.LangMast import lang
from sqlalchemy import func,distinct

class DailyConsumption(Stockout):
    type = 'DailyConsumption'

    def save_check(self, data, **kw):
        # 只能一次消耗
        clz = type(self)
        tmp = clz.query.filter(clz.CostCenterCode==data[0].get('costCenterCode'),
                            clz.TransDate==data[0].get('transDate'),
                            clz.BusinessType==clz.type)\
            .with_entities(func.count(distinct(clz.TransGuid))).first()
        if tmp[0] > 1:
            Error(lang('7F6D4A6B-8F9B-425E-82CE-5E4D6FC8A147') %(data[0].get('costCenterCode'),data[0].get('transDate')))

        return super().save_check(data, **kw)