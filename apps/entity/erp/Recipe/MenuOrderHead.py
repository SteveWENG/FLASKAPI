# -*- coding: utf-8 -*-

import pandas as pd
from flask import g
from pandas import merge
from sqlalchemy import Date, func, and_

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

    def list(self,costCenterCode,startDate):
        dates = self._dates(getDateTime(startDate))
        endDate = getDateTime(getDateTime(startDate) + datetime.timedelta(days=6))

        filters = [MenuOrderHead.CostCenterCode==costCenterCode,
                   MenuOrderHead.RequireDate>=startDate,
                   MenuOrderHead.RequireDate<=endDate]
        sql = MenuOrderHead.query.filter(*filters)\
            .join(CONTRACT,and_(MenuOrderHead.OrderLineGuid==CONTRACT.guid,
                                MenuOrderHead.RequireDate>=CONTRACT.StartDate,
                                MenuOrderHead.RequireDate<=CONTRACT.EndDate))\
            .outerjoin(MenuOrderFG,MenuOrderHead.HeadGuid==MenuOrderFG.HeadGuid)\
            .outerjoin(Product,MenuOrderFG.ItemGuid==Product.Guid) \
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
                                            sorted([k for k in tmp.columns if 'guid' in k])
                                            if x[c]][-1],axis=1)
        tmp['ClassName'] = tmp.apply(lambda x: '/'.join([x[c] for c in
                                                sorted([k for k in tmp.columns if 'ClassName' in k])
                                                if x[c]][:2]), axis=1)
        df = merge(df,tmp,how='left',left_on='CategoriesClassGUID',right_on='guid' )
        DataFrameSetNan(df)

        rms = MenuOrderRM.query.join(MenuOrderFG, MenuOrderRM.FGGuid == MenuOrderFG.FGGuid) \
            .join(MenuOrderHead, MenuOrderFG.HeadGuid == MenuOrderHead.HeadGuid).filter(*filters)\
            .join(Item, MenuOrderRM.ItemCode==Item.ItemCode) \
            .with_entities(MenuOrderRM.Id,MenuOrderRM.FGGuid, MenuOrderRM.ItemCode, Item.ItemName,MenuOrderRM.ItemUnit,
                           MenuOrderRM.ItemCost, MenuOrderRM.RequiredQty,MenuOrderRM.RequiredQty.label('RequiredQtyBak'),
                           MenuOrderRM.PurBOMConversion,MenuOrderRM.PurchasePolicy).all()
        rms = getdict(rms)

        ret = []
        for name1, group1 in df.groupby(by=['SOItemName', 'SOItemDesc','OrderLineGuid']):

            #加菜
            tmp = {k: {'MealQty':group1.iloc[0]['MealQty'],'MealPrice':group1.iloc[0]['MealPrice']}
                    for k,v in dates.items() if v>=group1.iloc[0]['StartDate']
                                                and v<=group1.iloc[0]['EndDate']}

            tmp['ItemName'] = name1[0]
            tmp['OrderLineGuid'] = name1[2]
            if name1[1]: tmp['ItemDesc'] = name1[1]
            ret.append(tmp)

            if group1.empty: continue

            cols = ['HeadId','HeadGuid','Id','ItemGuid','ItemCode','ItemName',
                    'ItemCost','ItemColor','ItemUnit','RequiredQty','PurchasePolicy']
            for name2,group2 in group1.groupby(by=['ClassName']):
                tmpg = []
                ignore = True # 没有记录
                for k,v in {k:v for k,v in dates.items()
                            if v>=group1.iloc[0]['StartDate']
                               and v<=group1.iloc[0]['EndDate']}.items():
                    g = group2[group2['RequireDate']==v]
                    if g.empty:
                        tmpg.append(pd.DataFrame([{k: ""}]))
                        continue

                    ignore = False
                    tmp = pd.DataFrame({k: g.apply(lambda x: {**{c: getVal(x[c]) for c in cols if x[c]},
                                                              'RMs': [{k: v for k, v in l.items()}
                                                                      for l in rms if l['FGGuid'] == x['FGGuid']]},
                                                   axis=1)}).reset_index(drop=True)
                    tmpg.append(tmp)
                if ignore: continue

                tmp = pd.concat(tmpg,axis=1)
                tmp['ItemName'] = name1[0]
                tmp['OrderLineGuid'] = name1[2]
                if name1[1]: tmp['ItemDesc'] = name1[1]
                if name2: tmp['ClassName'] = name2

                DataFrameSetNan(tmp)
                ret = ret + getdict(tmp, tmp.columns)

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
                tmpfg = MenuOrderFG({k:v if k.lower() != 'rms' else
                                            [{x:y for x,y in l.items() if x.lower() != 'fgguid'} for l in v]
                                     for k,v  in fg.items() if k.lower() != 'headguid'})

                tmpfg.CreatedUser = g.get('User')
                if not tmph.Id or not tmpfg.Id:
                    tmpfg.Id = None
                    tmpfg.FGGuid = getGUID()
                for rm in tmpfg.RMs:
                    if not tmpfg.Id:  rm.Id = None
                    rm.CreatedUser = g.get('User')

                '''
                if fg.get('RMs'):
                    tmpfg.RMs = []
                    for rm in fg['RMs']:
                        tmprm = MenuOrderRM(rm)
                        if not tmpfg.Id or not tmprm.Id:
                            tmprm.Id = None

                        tmpfg.RMs.append(tmprm)
                '''
                tmph.FGs.append(tmpfg)

            tmpg.append(tmph)
        with SaveDB() as session:
            for tmph in tmpg:
                session.merge(tmph)
            return lang('56CF8259-D808-4796-A077-11124C523F6F')
        x = 1