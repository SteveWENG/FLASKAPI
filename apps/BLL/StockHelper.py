# -*- coding: utf-8 -*-

import importlib

from ..entity.erp.common.Apps import Apps
from ..utils.functions import Error

from ..entity.erp.Stock.DailyConsumption import DailyConsumption

class StockHelper:

    @staticmethod
    def save( data):
        try:
            appGuid = data.get('appGuid','')
            if appGuid == '':
                Error('unknow app')

            clz = Apps.query.filter(Apps.Guid == appGuid).with_entities(Apps.ClassName).scalar()
            if clz == None:
                Error('unknow app')

            mod = importlib.import_module('.%s' %clz, package='apps.entity.erp.Stock')
            fun = getattr(getattr(mod, clz), 'save')
            return fun(data)
        except Exception as e:
            raise e

