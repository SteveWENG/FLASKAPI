# -*- coding: utf-8 -*-

import pandas as pd
from flask import g
from pandas import merge
from sqlalchemy import Date, func

from .ItemClass import ItemClass
from .MenuOrderRM import MenuOrderRM
from .MenuOrderFG import MenuOrderFG
from .Product import Product
from ..Item import Item
from ..Order.CONTRACT import CONTRACT
from ..common.LangMast import lang
from ... import SaveDB
from ....utils.functions import *
from ...erp import erp, db


class MenuOrderHead(erp):
    __tablename__ = 'tblMenuOrderHead'

    HeadGuid = db.Column()
    CostCenterCode = db.Column()
    OrderLineGuid = db.Column()
    ItemName = db.Column()
    ItemDesc = db.Column()
    RequireDate = db.Column(Date)
    MealQty = db.Column()
    MealPrice = db.Column()
    CreatedUser = db.Column()

    FGs = db.relationship('MenuOrderFG', primaryjoin='MenuOrderHead.HeadGuid == foreign(MenuOrderFG.HeadGuid)',
                          lazy='joined')

    def _dates(self, startDate):
        dates = {}
        for n in range(7):
            dates['day'+str(n)] = startDate + datetime.timedelta(days=n)
        return dates

    def list(self,costCenterCode,startDate, endDate):
        dates = self._dates(getDateTime(startDate))

        #df = pd.concat([df, pd.DataFrame(columns=dates.keys())])
        filters = [MenuOrderHead.CostCenterCode==costCenterCode,
                   MenuOrderHead.RequireDate>=startDate,
                   MenuOrderHead.RequireDate<=endDate]
        sql = MenuOrderHead.query.filter(*filters)\
            .outerjoin(MenuOrderFG,MenuOrderHead.HeadGuid==MenuOrderFG.HeadGuid)\
            .outerjoin(Product,MenuOrderFG.ItemGuid==Product.GUID) \
            .outerjoin(Item, MenuOrderFG.ItemCode == Item.ItemCode) \
            .with_entities(MenuOrderHead.Id.label('HeadId'),MenuOrderHead.HeadGuid,MenuOrderHead.OrderLineGuid,
                           MenuOrderHead.RequireDate,MenuOrderHead.MealQty,MenuOrderHead.MealPrice,
                           MenuOrderFG.Id,MenuOrderFG.FGGuid,MenuOrderFG.ItemGuid,
                           func.coalesce(MenuOrderFG.ItemCode,Product.ItemCode).label('ItemCode'),
                           func.coalesce(Product.ItemName,Item.ItemName).label('ItemName'),
                           Product.CategoriesClassGUID,
                           MenuOrderFG.ItemUnit,MenuOrderFG.RequiredQty,MenuOrderFG.ItemCost,
                           MenuOrderFG.ItemColor,MenuOrderFG.ItemCost,MenuOrderFG.PurchasePolicy)
        df = pd.read_sql(sql.statement,self.getBind())
        df = merge(CONTRACT.meals(costCenterCode,startDate,endDate),df,
                   how='left',left_on='guid',right_on='OrderLineGuid')
        if df.empty:
            Error(lang('D08CA9F5-3BA5-4DE6-9FF8-8822E5ABA1FF'))

        df.drop(['OrderLineGuid'],axis=1, inplace=True)
        df.rename(columns={'guid':'OrderLineGuid'},inplace=True)


        tmp = ItemClass.list()
        tmp['guid'] = tmp.apply(lambda x:  [x[c] for c in
                                            sorted([k for k in tmp.columns if 'guid' in k],reverse=True)
                                            if x[c]][0],axis=1)
        tmp['ClassName'] = tmp.apply(lambda x: '/'.join([x[c] for c in
                                                sorted([k for k in tmp.columns if 'ClassName' in k])
                                                if x[c]]), axis=1)
        df = merge(df,tmp,how='left',left_on='CategoriesClassGUID',right_on='guid' )
        DataFrameSetNan(df)

        rms = MenuOrderRM.query.join(MenuOrderFG, MenuOrderRM.FGGuid == MenuOrderFG.FGGuid) \
            .join(MenuOrderHead, MenuOrderFG.HeadGuid == MenuOrderHead.HeadGuid).filter(*filters)\
            .join(Item, MenuOrderRM.ItemCode==Item.ItemCode) \
            .with_entities(MenuOrderRM.Id,MenuOrderRM.FGGuid, MenuOrderRM.ItemCode, Item.ItemName,MenuOrderRM.ItemUnit,
                           MenuOrderRM.ItemCost, MenuOrderRM.RequiredQty,MenuOrderRM.PurBOMConversion,
                           MenuOrderRM.PurchasePolicy).all()
        rms = getdict(rms)

        ret = []
        for name, group in df.groupby(by=['SOItemName', 'SOItemDesc','OrderLineGuid','ClassName']):
            tmpg = []
            for k,v in dates.items():
                if v < group.iloc[0]['StartDate'] or v > group.iloc[0]['EndDate']:
                    continue

                tmp = pd.DataFrame([{k: {'MealQty':group.iloc[0]['MealQty'],'MealPrice':group.iloc[0]['MealPrice']}}])

                g = group.loc[group['RequireDate']==v,
                              ['HeadId','HeadGuid','Id','FGGuid',
                               'ItemGuid','ItemCode','ItemName','ItemCost','ItemColor','ItemUnit',
                               'RequiredQty','PurchasePolicy']]
                if g.empty:
                    tmpg.append(tmp)
                    continue

                tmp = tmp.append(pd.DataFrame({k: g.apply(lambda x: {**{c:getVal(x[c]) for c in g.columns if x[c]},
                                                          'RMs':[{k:v for k,v in l.items() if k not in ['FGGuid']}
                                                                 for l in rms if l['FGGuid']==x['FGGuid']]},
                                               axis=1)})).reset_index(drop=True)
                tmpg.append(tmp)

            tmp = pd.concat(tmpg,axis=1)
            tmp['ItemName'] = name[0]
            tmp['ItemDesc'] = name[1]
            tmp['OrderLineGuid'] = name[2]
            tmp['ClassName'] = name[3]

            DataFrameSetNan(tmp)
            tmpdict = getdict(tmp,tmp.columns)
            ret = ret + tmpdict

        return ret

    def save(self,data):
        tmpg = []
        dates = self._dates(getDateTime(data['date']))
        for l in data['orders']:
            date = [[k,v] for k,v in dates.items() if k in l.keys()]
            if not date: continue
            if len(date) > 1: Error('应该只有一条')

            tmph = MenuOrderHead(l)
            tmph.CostCenterCode = data['costCenterCode']
            tmph.RequireDate = date[0][1]
            tmph.CreatedUser = g.get('User')
            if not tmph.Id:
                tmph.Id = None
                tmph.HeadGuid = getGUID()

            for fg in l[date[0][0]]:
                tmpfg = MenuOrderFG(fg)
                if not tmph.Id or not tmpfg.Id:
                    tmpfg.Id = None
                    tmpfg.FGGuid = getGUID()

                if fg.get('RMs'):
                    tmpfg.RMs = []
                    for rm in fg['RMs']:
                        tmprm = MenuOrderRM(rm)
                        if not tmpfg.Id or not tmprm.Id:
                            tmprm.Id = None

                        tmpfg.RMs.append(tmprm)

                tmph.FGs.append(tmpfg)

            tmpg.append(tmph)
        with SaveDB() as session:
            for tmph in tmpg:
                session.merge(tmph)
            return lang('56CF8259-D808-4796-A077-11124C523F6F')
        x = 1