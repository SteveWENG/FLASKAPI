# -*- coding: utf-8 -*-
import importlib
from operator import itemgetter
from itertools import groupby
import pandas as pd

from ..entity.EXACT.gbkmut import gbkmut
from ..entity.erp import Stock
from ..entity.erp.Stock import TransData
from ..entity.erp.common.Apps import Apps
from ..entity.erp.common.CCMast import CCMast
from ..entity.erp.common.CostCenter import CostCenter
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
            clz = getattr(mod,clz)
            fun = getattr(clz, FuncName)
            # fun = getattr(getattr(mod, clz)(), FuncName)
            return fun(data)
        except Exception as e:
            raise e

    @staticmethod
    def UpdateOpenningStock(data):
        try:
            codes = [getStr(s) for s in data.get('costCenterCodes','').split(',') if getStr(s)]
            divisions = [getStr(s) for s in data.get('divisions','').split(',') if getStr(s)]
            if divisions:
                codes = [l.get('CostCenterCode','') for l in CostCenter.Sites(divisions,None)] #CCMast.ShowSites(divisions,None)]
            tmpcodes = TransData.query.filter(TransData.CostCenterCode.in_(codes),
                                              TransData.BusinessType=='OpenningStock')\
                .with_entities(TransData.CostCenterCode).distinct().all()

            # 未导入Openning stock的sites
            tmpcodes = list(set(codes).difference(set([getStr(s.CostCenterCode) for s in tmpcodes])))
            if len(tmpcodes) == 0 :
               return 'Already updated their openning stock'

            # tmpsql = CCMast.query.filter(CCMast.CostCenterCode.in_(tmpcodes)).distinct(CCMast.DBName,CCMast.CostCenterCode)
            exactWarehouses = CostCenter.Sites(None,tmpcodes)
            exactWarehouses = [{'db':key,
                                'sites':[{'CostCenterCode':l.get('CostCenterCode'),'warehouse':l.get('ExactWarehouse')}
                                         for l in list(group)  if l.get('ExactWarehouse','') != '']}
                               for key,group in groupby(exactWarehouses,itemgetter('Division'))]
            exactWarehouses = [l for l in exactWarehouses if len(l.get('sites',[])) > 0]
            tmpList = gbkmut.ClosingStock(exactWarehouses)

            return TransData.UpdateOpenningStock(tmpList)
        except Exception as e:
            raise e


    @staticmethod
    def list(data):
        with RunApp():
            return TransData.list(data);