# -*- coding: utf-8 -*-

from functools import reduce

import pandas as pd
from pandas import merge
from sqlalchemy import func
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.sql.functions import current_user

from ..common.DataControlConfig import DataControlConfig
from ... import SaveDB
from ....utils.MyProcess import MyProcess
from ....utils.functions import *
from .ItemClass import ItemClass
from ..Item.PriceList import PriceList
from ..common.CostCenter import CostCenter
from ..common.LangMast import lang
from ...erp import erp, db,CurrentUser
from .ItemBOM import ItemBOM

class Product(erp):
    __tablename__ = 'tblProduct'

    Division = db.Column('company')
    Guid = db.Column()
    ItemCode = db.Column()
    ItemName = db.Column('ItemName_ZH')
    ItemCost = db.Column()
    ShareQty = db.Column()
    CategoriesClassGuid = db.Column()
    CookwayClassGuid = db.Column()
    SeasonClassGuid = db.Column()
    ItemShapeGuid = db.Column()
    Status = db.Column()
    CreateUser = db.Column(default=CurrentUser)
    CreateTime = db.Column(server_default='getdate()')
    ChangedUser = db.Column(default=CurrentUser, onupdate=CurrentUser)
    ChangedTime = db.Column(default=datetime.datetime.now, onupdate=datetime.datetime.now)

    ItemBOM = db.relationship('ItemBOM', primaryjoin='Product.Guid == foreign(ItemBOM.ProductGuid)',
                          lazy='joined')

    @hybrid_property
    def ItemName1(self):
        return self.LangColumn('ItemName_')

    @classmethod
    def list(cls,division, costCenterCode, date,fortype):
        fortype = getStr(fortype).lower()
        if (not division and not costCenterCode) or (fortype=='menu' and not costCenterCode):
            Error(lang('D08CA9F5-3BA5-4DE6-9FF8-8822E5ABA1FF'))  # No data

        if division:
            if not costCenterCode: fortype = 'recipe'
        else:
            division = CostCenter.GetDivision(costCenterCode)

        def _list(bind,division, costCenterCode,date,fortype):
            filters = [cls.Division==division, cls.Status=='active',ItemBOM.DeleteTime==None,
                       ~cls.ItemBOM.any(ItemBOM.ItemCode.like('[AB]%'))]
            fields = [cls.CategoriesClassGuid,cls.CookwayClassGuid,cls.SeasonClassGuid,cls.ItemShapeGuid,
                      cls.Guid.label('ProductGuid'),cls.ItemCode.label('ProductCode'),
                      cls.ItemName.label('ProductName'),cls.ShareQty,cls.CreateUser,cls.CreateTime]
            fields += ItemBOM.listCols(fortype)

            if costCenterCode:
               filters.append(func.coalesce(ItemBOM.CostCenterCode,costCenterCode)==costCenterCode)
            # BOM
            sql = cls.query.filter(*filters).join(ItemBOM,cls.Guid==ItemBOM.ProductGuid)
            sql = sql.with_entities(*fields)
            product = MyProcess(pd.read_sql,sql.statement,bind)

            pricelist = None
            if fortype == 'menu':
                pricelist = MyProcess(PriceList.list, division, costCenterCode, date, 'Food', False)
                pricelist = pricelist.get()
                if pricelist.empty:
                    Error(lang('D08CA9F5-3BA5-4DE6-9FF8-8822E5ABA1FF'))  # No data

                # PriceList
                pricelist['PurBOMConversion'] = pricelist['PurStkConversion'] * pricelist['StkBOMConversion']
                pricelist.drop(['PurStkConversion', 'StkBOMConversion', 'StockUnit'], axis=1, inplace=True)

            product = product.get()
            if product.empty:
                Error(lang('D08CA9F5-3BA5-4DE6-9FF8-8822E5ABA1FF'))  # No data

            DataFrameSetNan(product)
            product.loc[product['CostCenterCode']!=costCenterCode,'Id'] = ''
            if fortype == 'recipe':
                product.rename(columns={'ItemCost':'Price','ConversionRate':'PurBOMConversion','UOM':'BOMUnit'},
                               inplace=True)
                return product

            siteproduct = product[product['CostCenterCode']==costCenterCode]
            if getStr(DataControlConfig.getMenuRecipeBy()).lower() == 'sitedrecipes': # 近取Site的Recipe
                if siteproduct.empty:
                    Error(lang('D08CA9F5-3BA5-4DE6-9FF8-8822E5ABA1FF'))  # No data
                product = siteproduct
            elif not siteproduct.empty: # 有site的recipe则取site的，否则取division的
                product = product[~product['ProductGuid'].isin(set(siteproduct['ProductGuid']))].append(siteproduct)

            df = merge(product, pricelist, how='outer', left_on='ItemCode', right_on='ItemCode')
            DataFrameSetNan(df)
            df['ItemType'] = df['ProductCode'].map(lambda x: 'FG' if x else 'RM')
            return df

        df = _list(cls.getBind(),division,costCenterCode,date,fortype)
        DataFrameSetNan(df)

        # Product -> BOM
        def _recipes(df):
            product = df
            if 'ItemType' in df.columns:
                product = df[df['ItemType'] == 'FG']

            groupbyFields = ['CategoriesClassGuid', 'CookwayClassGuid','SeasonClassGuid', 'ItemShapeGuid', 'ProductGuid',
                             'ProductCode', 'ProductName', 'ShareQty', 'CreateUser', 'CreateTime', 'ItemType']

            product = product.groupby(by=list(set(groupbyFields).intersection(set(product.columns))))\
                .apply(lambda x: pd.Series({'ItemCost': (x['PurPrice']*x['Qty']/x['PurBOMConversion']).sum(),
                                            'ItemBOM': reduce(lambda x1, x2: x1 + x2,
                                                               [[{k: v for k, v in l.items()
                                                                  if k not in groupbyFields and v}]
                                                                for l in x.sort_values(by=['ItemCode'])
                                                               .to_dict('records')])}))\
                .reset_index().rename(columns={'ProductGuid':'ItemGuid',
                                               'ProductCode':'ItemCode',
                                               'ProductName':'ItemName'})
            product['ItemUnit'] = 'pc'

            # Product category
            itemClass =ItemClass.list(2)
            for l in ['CategoriesClass','CookwayClass','SeasonClass','ItemShape']:
                product = merge(product,itemClass,how='left',left_on=(l+'Guid'),right_on='guid')\
                    .drop(['guid'],axis=1)\
                    .rename(columns={'ClassName':l+'Name','Sort':l+'Sort'})
            return product

        product = _recipes(df)

        if 'ItemType' in df.columns:
            rms = df.loc[df['ItemType'] == 'RM',
                         ['ClassCode', 'ClassName', 'ItemCode', 'ItemName', 'PurUnit', 'PurPrice', 'ItemType']] \
                .rename(columns={'PurUnit': 'ItemUnit', 'PurPrice': 'ItemCost',
                                 'ClassCode': 'CategoriesClassGuid', 'ClassName': 'CategoriesClassName'})
            product = product.append(rms)

        DataFrameSetNan(product)
        ttt = [t for t in ['ItemType','CategoriesClassSort', 'ItemCode'] if t in set(product.columns)]
        return getdict(product.sort_values(by=ttt))

    def save(self):
        # New or modify

        if not self.Id:
            self.Id = None
            self.Guid = getGUID()
            for bom in self.ItemBOM:
                bom.Id = None

        with SaveDB() as session:
            session.merge(self)
            return lang('56CF8259-D808-4796-A077-11124C523F6F')