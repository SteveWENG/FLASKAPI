# -*- coding: utf-8 -*-
import importlib

from ..entity.EXACT.gbkmut import gbkmut
from ..entity.erp.Stock import TransData
from ..entity.erp.common.Apps import Apps
from ..entity.erp.common.CCMast import CCMast
from ..utils.functions import Error



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

    @staticmethod
    def UpdateOpenningStock(data):
        try:
            costCenterCode = data.get('costCenterCode')
            if TransData.query.filter(TransData.CostCenterCode == costCenterCode).first() != None:
                return 'Already updated the openning stock of ' + costCenterCode

            dbName = CCMast.query.filter(CCMast.CostCenterCode == costCenterCode)\
                .with_entities(CCMast.DBName).scalar()

            tmpList = gbkmut.ClosingStock(dbName,costCenterCode)

            return TransData.UpdateOpenningStock(costCenterCode,data.get('creater',''),tmpList)
        except Exception as e:
            raise e