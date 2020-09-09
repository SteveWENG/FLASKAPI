# -*- coding: utf-8 -*-

import pandas as pd


from ..common.LangMast import lang
from ....utils.functions import Error
from .Stockout import Stockout

class DailyTicket(Stockout):
    type = 'DailyTicket'

    def SaveData(self, trans, **kw):

        trans['qty'] = -trans['qty']
        return trans #tmp.to_dict('records')

    def save_check(self, data, **kw):
        self.CheckOrderLine(data)
        return lang('7B578615-3038-4691-BD5E-5093C62E36E4') # Successfully saved daily tickets