# -*- coding: utf-8 -*-
from ..entity.erp.Item.PriceList import PriceList
from ..entity.erp.common.LangMast import lang
from ..utils.functions import *


class ItemHelper:

    @staticmethod
    def PriceList(data):
        costCenterCode = data.get('costCenterCode')
        date = data.get('date')
        type = data.get('type')
        if not costCenterCode or not date or not type:
            Error(lang('No data'))

        return getdict(PriceList.list(costCenterCode,date,type))