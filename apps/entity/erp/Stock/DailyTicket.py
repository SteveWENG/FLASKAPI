# -*- coding: utf-8 -*-
from sqlalchemy import distinct, func

from ..common.LangMast import lang
from ....utils.functions import Error
from .Stockout import Stockout

class DailyTicket(Stockout):
    type = 'DailyTicket'

    def save_check(self, data, **kw):
        self.CheckOrderLine(data)
        return lang('7B578615-3038-4691-BD5E-5093C62E36E4') # Successfully saved daily tickets
