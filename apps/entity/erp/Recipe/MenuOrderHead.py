# -*- coding: utf-8 -*-

import pandas as pd
from flask import g
from pandas import merge
from sqlalchemy import func
import datetime

from .ItemClass import ItemClass
from .MenuOrderRM import MenuOrderRM
from .MenuOrderFG import MenuOrderFG
from .Product import Product
from ..Item import Item
from ..Order.CONTRACT import CONTRACT
from ..common.LangMast import lang
from ... import SaveDB
from ....utils.functions import *
from ...erp import erp, db, CurrentUser


class MenuOrderHead(erp):
    __tablename__ = 'tblMenuOrderHead'

    HeadGuid = db.Column()
    CostCenterCode = db.Column()
    OrderLineGuid = db.Column()
    ItemName = db.Column()
    ItemDesc = db.Column()
    RequireDate = db.Column()
    MealQty = db.Column()
    MealPrice = db.Column()
    CreatedUser = db.Column(default=CurrentUser)
    ChangedUser = db.Column(default=CurrentUser, onupdate=CurrentUser)
    ChangedTime = db.Column(default=datetime.datetime.now, onupdate=datetime.datetime.now)

    FGs = db.relationship('MenuOrderFG', primaryjoin='MenuOrderHead.HeadGuid == foreign(MenuOrderFG.HeadGuid)',
                          lazy='joined')

    def _dates(self, startDate):
        if isinstance(startDate,str): startDate = getDateTime(startDate)
        dates = {}
        for n in range(7):
            dates['day'+str(n)] = startDate + datetime.timedelta(days=n)
        return dates

    def _list(self,costCenterCode,startDate):
        endDate = getDateTime(getDateTime(startDate) + datetime.timedelta(days=6))

        filters = [MenuOrderHead.CostCenterCode == costCenterCode,
                   MenuOrderHead.RequireDate >= startDate,
                   MenuOrderHead.RequireDate <= endDate]
        return MenuOrderHead.query.filter(*filters),filters,endDate

    # startDate：一周的周一
    def list(self,costCenterCode,startDate):
        sql,filters,endDate = self._list(costCenterCode,startDate)

        sql = sql.outerjoin(MenuOrderFG,MenuOrderHead.HeadGuid==MenuOrderFG.HeadGuid)\
            .outerjoin(Product,MenuOrderFG.ItemGuid==Product.Guid) \
            .outerjoin(Item, MenuOrderFG.ItemCode == Item.ItemCode) \
            .with_entities(MenuOrderHead.OrderLineGuid,
                           MenuOrderHead.RequireDate,MenuOrderHead.MealQty,MenuOrderHead.MealPrice,
                           MenuOrderFG.Id,MenuOrderFG.FGGuid,MenuOrderFG.ItemGuid,
                           func.coalesce(MenuOrderFG.ItemCode,Product.ItemCode).label('ItemCode'),
                           func.coalesce(Product.ItemName,Item.ItemName).label('ItemName'),
                           Product.CategoriesClassGuid,
                           MenuOrderFG.ItemUnit,MenuOrderFG.RequiredQty,MenuOrderFG.ItemCost,
                           MenuOrderFG.ItemColor,MenuOrderFG.ItemCost,MenuOrderFG.PurchasePolicy)
        df = pd.read_sql(sql.statement,self.getBind())

        dfmeals = CONTRACT.meals(costCenterCode,startDate,endDate)

        dates = self._dates(startDate)
        mealcols = ['MealQty', 'MealPrice']

        # 新增
        if df.empty:
            if dfmeals.empty:
                Error(lang('D08CA9F5-3BA5-4DE6-9FF8-8822E5ABA1FF'))

            for k,v in dates.items():
                dfmeals.loc[(dfmeals['StartDate']<=v)&(dfmeals['EndDate']>=v),k] = \
                    dfmeals.apply(lambda x:{c:'' for c in mealcols})
            DataFrameSetNan(dfmeals)

            return getdict(dfmeals.sort_values(by=['LineNum'])
                           .drop(['StartDate', 'EndDate','LineNum'], axis=1)
                           .rename(columns={'SOItemName': 'ItemName', 'SOItemDesc': 'ItemDesc','guid':'OrderLineGuid'}))

        df = merge(dfmeals, df, how='left', left_on='guid', right_on='OrderLineGuid')
        df.drop(['OrderLineGuid'],axis=1, inplace=True)
        df.rename(columns={'guid':'OrderLineGuid'},inplace=True)

        # Product的分类
        df = merge(df,ItemClass.list(2).rename(columns={'Sort':'ClassSort'}),
                   how='left',left_on='CategoriesClassGuid',right_on='guid' )\
            .rename(columns={'CategoriesClassGuid':'ClassGuid'})

        DataFrameSetNan(df)

        rms = MenuOrderRM.query.join(MenuOrderFG, MenuOrderRM.FGGuid == MenuOrderFG.FGGuid) \
            .join(MenuOrderHead, MenuOrderFG.HeadGuid == MenuOrderHead.HeadGuid).filter(*filters) \
            .join(Item, MenuOrderRM.ItemCode == Item.ItemCode) \
            .with_entities(MenuOrderRM.Id, MenuOrderRM.FGGuid, MenuOrderRM.ItemCode,
                           Item.ItemName, MenuOrderRM.PurUnit,MenuOrderRM.BOMUnit,
                           MenuOrderRM.BOMQty,MenuOrderRM.ItemPrice, MenuOrderRM.RequiredQty,
                           MenuOrderRM.PurBOMConversion, MenuOrderRM.PurchasePolicy)
        rms = pd.read_sql(rms.statement,self.getBind())

        df.loc[df['FGGuid'] != '', 'RMs'] = df.apply(lambda x: getdict(rms[rms['FGGuid'] == x['FGGuid']]), axis=1)

        # group, day0,day1...对齐
        def _groupdf(li, groupbyFields, aggcols,keepEmpty=False):
            if li.empty: return li
            for k, v in dates.items():
                li.loc[(li['RequireDate'] == v), k] = li.apply(lambda x: {c: x[c] for c in aggcols if x[c]}, axis=1)

            def _g(g,dayX):
                g1 = g[~g[dayX].isna()]
                if keepEmpty and g1.empty:
                    return [{c:'' for c in aggcols}]

                return g1[dayX].reset_index(drop=True)

            return li.groupby(by=groupbyFields) \
                .apply(lambda g: pd.DataFrame({k: _g(g,k) for k,v in dates.items()
                                               if g.iloc[0]['StartDate']<=v and g.iloc[0]['EndDate']>=v})) \
                .reset_index()

        def _processdf(df):
            groupbyFields = ['LineNum', 'SOItemName', 'SOItemDesc', 'OrderLineGuid',
                             'StartDate', 'EndDate']
            tdf = _groupdf(df.drop_duplicates(subset=(groupbyFields + ['RequireDate'])),
                            groupbyFields, mealcols,True)

            groupbyFields += ['ClassGuid', 'ClassName','ClassSort']
            fgcols = ['Id', 'FGGuid', 'ItemGuid', 'ItemCode', 'ItemName', 'ItemCost',
                    'ItemColor', 'ItemUnit', 'RequiredQty', 'PurchasePolicy','RMs']
            tdf1 = _groupdf(df[df['Id'] > 0], groupbyFields, fgcols)
            if not tdf1.empty:
                tdf1.fillna(value={k: '' for k in dates.keys()}, inplace=True)
                tdf = tdf1.append(tdf[set(tdf.columns).intersection(set(tdf1.columns))])

            DataFrameSetNan(tdf)
            return tdf.sort_values(by=list(set(['LineNum','ClassSort']).intersection(set(tdf.columns))))
            [list(set(groupbyFields).intersection(set(tdf.columns))) + list(dates.keys())]

        tdf = _processdf(df).drop(['StartDate', 'EndDate'], axis=1)\
            .rename(columns={'SOItemName': 'ItemName', 'SOItemDesc': 'ItemDesc'})

        return getdict(tdf)

    def save_bak(self,data):
        costCenterCode = data['costCenterCode']
        startDate = data['date']
        menuorderheads = self._list(costCenterCode,startDate)[0]\
            .with_entities(MenuOrderHead.Id,MenuOrderHead.HeadGuid,
                           MenuOrderHead.OrderLineGuid,MenuOrderHead.RequireDate).all()

        dates = self._dates(getDateTime(startDate))
        tmpg = []

        for l in data['orders']:
            date = [[k,v] for k,v in dates.items() if k in l.keys()]
            if not date: continue
            if len(date) > 1:
                Error('一个OrderLineGuid中应该只有一个日期')

            tmph = MenuOrderHead(l)
            tmph.CostCenterCode = costCenterCode
            tmph.RequireDate = date[0][1]

            head = [h for h in menuorderheads
                    if h.OrderLineGuid==tmph.OrderLineGuid
                    and h.RequireDate==tmph.RequireDate]

            if head:
                tmph.Id = head[0].Id
                tmph.HeadGuid = head[0].HeadGuid
            else:
                tmph.HeadGuid = getGUID()

            for fg in l[date[0][0]]:
                tmpfg = MenuOrderFG({k:v if k.lower() != 'rms' else
                                            [{x:y for x,y in l.items() if x.lower() != 'fgguid'} for l in v]
                                     for k,v  in fg.items() if k.lower() != 'headguid'})

                if not tmph.Id or not tmpfg.Id:
                    tmpfg.Id = None
                    tmpfg.FGGuid = getGUID()
                for rm in tmpfg.RMs:
                    if not tmpfg.Id:  rm.Id = None

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

    def save(self,data):
        costCenterCode = data['costCenterCode']
        startDate = data['date']
        menuorderheads = self._list(costCenterCode,startDate)[0]\
            .with_entities(MenuOrderHead.Id,MenuOrderHead.HeadGuid,
                           MenuOrderHead.OrderLineGuid,MenuOrderHead.RequireDate).all()

        dates = self._dates(getDateTime(startDate))
        tmpg = []

        for l in data['orders']:
            tdates = [{k:v} for k,v in dates.items() if k in l.keys()]
            if not tdates: continue
            # if len(date) > 1: Error('一个OrderLineGuid中应该只有一个日期')
            for dic in tdates:
                tmph = MenuOrderHead(l)
                tmph.CostCenterCode = costCenterCode
                tmph.RequireDate = list(dic.values())[0]

                head = [h for h in menuorderheads
                        if h.OrderLineGuid==tmph.OrderLineGuid
                        and h.RequireDate==tmph.RequireDate]

                if head:
                    tmph.Id = head[0].Id
                    tmph.HeadGuid = head[0].HeadGuid
                else:
                    tmph.HeadGuid = getGUID()

                for fg in l[list(dic.keys())[0]]:#l[date[0][0]]:
                    tmpfg = MenuOrderFG({k:v if k.lower() != 'rms' else
                                                [{x:y for x,y in l.items() if x.lower() != 'fgguid'} for l in v]
                                         for k,v  in fg.items() if k.lower() != 'headguid'})

                    if not tmph.Id or not tmpfg.Id:
                        tmpfg.Id = None
                        tmpfg.FGGuid = getGUID()
                    for rm in tmpfg.RMs:
                        if not tmpfg.Id:  rm.Id = None

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