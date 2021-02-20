# -*- coding: utf-8 -*-

from ..Order.CONTRACT import CONTRACT
from ..common.LangMast import lang,getParameters
from ....utils.functions import *
from .Stockout import Stockout

class DailyTicket(Stockout):
    type = 'DailyTicket'

    @classmethod
    def dates(cls,data):
        costCenterCode = getParameters(data,['costCenterCode'])
        today = datetime.date.today()
        date = today+ datetime.timedelta(days=-15)
        date = min(datetime.date(today.year,today.month,1),date)
        tmp = cls.query.filter(cls.CostCenterCode == costCenterCode,
                               cls.BusinessType==cls.type,
                               cls.TransDate>=date,cls.DeleteTime==None)\
            .with_entities(cls.TransDate).all()
        if tmp:
            tmp = [t.TransDate for t in tmp]

        return [{'date':getDateTime(today+datetime.timedelta(days=-x)),
                 'Remark':lang('18B66397-D664-4B66-A8F0-CE048399A972')
                 if tmp and (today+datetime.timedelta(days=-x)) in tmp else ''}
                for x in range((today-date).days+1)][::-1]


    @classmethod
    def items(cls, data):
        costCenterCode = data.get('costCenterCode', '')
        date = data.get('date', '')
        if not costCenterCode or not date:
            Error(lang('D08CA9F5-3BA5-4DE6-9FF8-8822E5ABA1FF'))  # No data

        df = CONTRACT.list(costCenterCode,date)
        # df['LineNum'] = df['LineNum'].apply(str)
        df.rename(columns={'guid':'orderLineGuid','UnitPrice':'itemPrice','Unit':'uom',
                           'ItemCode':'itemCode','ItemName':'itemName'},inplace=True)

        return [{**d,'isServiceItem': True} for d in getdict(df)]

    @classmethod
    def SaveData(cls, trans, **kw):
        trans = trans[trans['qty']>0]
        trans['qty'] = -trans['qty']
        return trans #tmp.to_dict('records')

    @classmethod
    def save_check(cls, data, **kw):
        cls.CheckOrderLine(data,**kw)
        return lang('7B578615-3038-4691-BD5E-5093C62E36E4') # Successfully saved daily tickets
