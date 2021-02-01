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

    def list(self,costCenterCode,startDate):
        sql,filters,endDate = self._list(costCenterCode,startDate)

        sql = sql.join(CONTRACT,and_(MenuOrderHead.OrderLineGuid==CONTRACT.guid,
                                MenuOrderHead.RequireDate>=CONTRACT.StartDate,
                                MenuOrderHead.RequireDate<=CONTRACT.EndDate))\
            .outerjoin(MenuOrderFG,MenuOrderHead.HeadGuid==MenuOrderFG.HeadGuid)\
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
        isNewMenu = df.empty
        if isNewMenu:
            df = dfmeals
        else:
            df = merge(dfmeals, df, how='left', left_on='guid', right_on='OrderLineGuid')
            df.drop(['OrderLineGuid'],axis=1, inplace=True)
            df.rename(columns={'guid':'OrderLineGuid'},inplace=True)

            tmp = ItemClass.list(2).rename(columns={'Sort':'ClassSort'})
            df = merge(df,tmp,how='left',left_on='CategoriesClassGuid',right_on='guid' )

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
        def _groupdf(li, dates, groupbyFields, aggcols):
            for k, v in dates.items():
                li.loc[(li['RequireDate'] == v), k] = li.apply(lambda x: {c: x[c] for c in aggcols if x[c]}, axis=1)

            return li.groupby(by=groupbyFields) \
                .apply(lambda g: pd.DataFrame({k: g.loc[~g[k].isna(), k].reset_index(drop=True)
                                               for k in dates.keys()})) \
                .reset_index()

        def _processdf(df, startDate, newMenu):
            dates = self._dates(startDate)
            cols = ['MealQty', 'MealPrice']
            sortFields = ['SOItemName', 'SOItemDesc']

            def _datecols(df,dates):
                for k,v in dates.items():
                    tmpf = (df['StartDate'] <= v) & (df['EndDate'] >= v)
                    if k in list(df.columns): tmpf &= (df[k].isna())  # 无效的订单行

                    df.loc[tmpf, k] = df.apply(lambda x: {c: '' for c in cols}, axis=1)
                    df.loc[(df['StartDate'] > v) | (df['EndDate'] < v), k] = math.nan
                return df

            # MenuOrder没有数据
            if newMenu:
                df = _datecols(df,dates)
                DataFrameSetNan(df)
                return df.sort_values(by=sortFields)

            groupbyFields = ['SOItemName', 'SOItemDesc', 'OrderLineGuid', 'StartDate', 'EndDate']
            tdf1 = _groupdf(df.drop_duplicates(subset=(groupbyFields + ['RequireDate'])),
                                 dates, groupbyFields, cols)
            tdf1 = tdf1.append(df.loc[df['RequireDate'] == '', groupbyFields]).reset_index()

            '''
            for k, v in dates.items():
                tdf1.loc[(tdf1['StartDate']<=v) & (tdf1['EndDate']>=v) & (tdf1[k].isna()), k] =\
                    tdf1.apply(lambda x: {c: '' for c in cols},axis=1)
                tdf1.loc[(tdf1['StartDate'] > v) | (tdf1['EndDate'] < v), k] = math.nan
            '''
            tdf1 = _datecols(tdf1,dates)
            cols = ['Id', 'FGGuid', 'ItemGuid', 'ItemCode', 'ItemName', 'ItemCost',
                    'ItemColor', 'ItemUnit', 'RequiredQty', 'PurchasePolicy','RMs']
            groupbyFields = ['SOItemName', 'SOItemDesc', 'OrderLineGuid', 'ClassName', 'ClassSort']
            tdf = _groupdf(df[df['Id'] > 0], dates, groupbyFields, cols)
            tdf.fillna(value={k: '' for k in dates.keys()}, inplace=True)

            tdf1 = tdf.append(tdf1[set(tdf1.columns).intersection(set(tdf.columns))])
            DataFrameSetNan(tdf1)
            sortFields.append('ClassSort')

            return tdf1.sort_values(by=sortFields)[groupbyFields + list(dates.keys())]


        tdf = _processdf(df, startDate, isNewMenu)\
            .rename(columns={'SOItemName': 'ItemName', 'SOItemDesc': 'ItemDesc'})

        return getdict(tdf)

    def save(self,data):
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
            if len(date) > 1: Error('应该只有一条')

            tmph = MenuOrderHead(l)
            tmph.CostCenterCode = costCenterCode
            tmph.RequireDate = date[0][1]
            tmph.CreatedUser = g.get('User')

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