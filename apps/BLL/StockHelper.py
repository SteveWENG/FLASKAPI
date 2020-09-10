# -*- coding: utf-8 -*-
import importlib

import pandas as pd

from ..entity.EXACT.gbkmut import gbkmut
from ..entity.erp.Stock import TransData
from ..entity.erp.common.Apps import Apps
from ..entity.erp.common.CCMast import CCMast
from ..utils.functions import *



class StockHelper:

    @staticmethod
    def save(data):
        try:
            return StockHelper._run('save',data)
        except Exception as e:
            raise e

    @staticmethod
    def items(data):
        try:
            return StockHelper._run('items',data)
        except Exception as e:
            raise e

    @staticmethod
    def _run(FuncName, data):
        try:
            appGuid = data.get('appGuid','')
            if appGuid == '':
                Error('unknow app')

            clz = Apps.query.filter(Apps.Guid == appGuid).with_entities(Apps.ClassName).scalar()
            if clz == None:
                Error('unknow app')

            mod = importlib.import_module('.%s' %clz, package='apps.entity.erp.Stock')

            # 反射类
            # fun = getattr(getattr(mod, clz), 'save')
            fun = getattr(getattr(mod, clz)(), FuncName)
            return fun(data)
        except Exception as e:
            raise e

    @staticmethod
    def UpdateOpenningStock(data):
        try:
            codes = [getStr(s) for s in data.get('costCenterCodes','').split(',') if getStr(s)]
            divisions = [getStr(s) for s in data.get('divisions','').split(',') if getStr(s)]
            if divisions:
                codes = [l.get('CostCenterCode','') for l in CCMast.ShowSites(divisions,None)]
            tmpcodes = TransData.query.filter(TransData.CostCenterCode.in_(codes),
                                              TransData.BusinessType=='OpenningStock')\
                .with_entities(TransData.CostCenterCode).distinct().all()
            tmpcodes = list(set(codes).difference(set([getStr(s.CostCenterCode) for s in tmpcodes])))
            if len(tmpcodes) == 0 :
               return 'Already updated their openning stock'

            tmpsql = CCMast.query.filter(CCMast.CostCenterCode.in_(tmpcodes))\
                .distinct(CCMast.DBName,CCMast.CostCenterCode)
            tmpList = pd.read_sql(tmpsql.statement, CCMast.getBind())
            tmpList = [{'db': k, 'sites': ls.CostCenterCode.tolist()}
                      for k, ls in tmpList.groupby('DBName',as_index=False)]

            tmpList = gbkmut.ClosingStock(tmpList)

            return TransData.UpdateOpenningStock(tmpList)
        except Exception as e:
            raise e