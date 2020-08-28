# -*- coding: utf-8 -*-
from sqlalchemy import distinct, func

from ..common.LangMast import lang
from ....utils.functions import Error
from .Stockout import Stockout

class DailyTicket(Stockout):
    type = 'DailyTicket'

    def save_check(self, data):
        self.CheckOrderLine(data)
        return lang('7B578615-3038-4691-BD5E-5093C62E36E4') # Successfully saved daily tickets

    def GetOrderLine(self, lineGuids, date):
        clz = type(self)
        return clz.query.filter(clz.OrderLineGuid.in_(lineGuids),clz.TransDate==date)\
            .with_entities(func.count(distinct(clz.OrderLineGuid)).label('GuidCount'),  # 全部save
                           func.count(clz.Id).label('TotalCount')).first()  # 重复save