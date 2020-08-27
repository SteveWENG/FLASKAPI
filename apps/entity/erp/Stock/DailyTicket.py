# -*- coding: utf-8 -*-
from ..common.LangMast import LangMast
from ....utils.functions import Error
from .Stockout import Stockout

class DailyTicket(Stockout):
    type = 'DailyTicket'

    @classmethod
    def save_check(cls, data):
        cls.CheckOrderLine(data)
        return LangMast.getText('7B578615-3038-4691-BD5E-5093C62E36E4') # Successfully saved daily tickets