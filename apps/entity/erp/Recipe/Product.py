# -*- coding: utf-8 -*-
import threading
from functools import reduce

import pandas as pd
from flask import current_app
from pandas import merge
from sqlalchemy import func, and_
from sqlalchemy.ext.hybrid import hybrid_property

from ....utils.MyProcess import MyProcess
from ....utils.functions import *
from .ItemClass import ItemClass
from ..Item.PriceList import PriceList
from ..common.CostCenter import CostCenter
from ..common.LangMast import lang
from ...erp import erp, db
from .ItemBOM import ItemBOM

class Product(erp):
    __tablename__ = 'tblProduct'

    Division = db.Column('company')
    Guid = db.Column()
    ItemCode = db.Column()
    ShareQty = db.Column()
    CategoriesClassGuid = db.Column()
    CookwayClassGuid = db.Column()
    ItemShapeGuid = db.Column()
    Status = db.Column()
    CreateUser = db.Column()
    CreateTime = db.Column(server_default='getdate()')

    BOMs = db.relationship('ItemBOM', primaryjoin='Product.Guid == foreign(ItemBOM.ProductGuid)',
                          lazy='joined')

    @hybrid_property
    def ItemName(self):
        return self.LangColumn('ItemName_')

    @classmethod
    def list(cls,division, costCenterCode, date):
        if not division:
            division = CostCenter.GetDivision(costCenterCode)

        # BOM
        sql = cls.query.filter(cls.Division==division, cls.Status=='active',ItemBOM.DeleteTime==None,
                               func.coalesce(ItemBOM.CostCenterCode,costCenterCode)==costCenterCode,
                               ~cls.BOMs.any(ItemBOM.ItemCode.like('[AB]%')))\
            .join(ItemBOM,cls.Guid==ItemBOM.ProductGuid)\
            .with_entities(cls.CategoriesClassGuid,cls.CookwayClassGuid,cls.ItemShapeGuid,
                           cls.Guid.label('ProductGuid'),cls.ItemCode.label('ProductCode'),
                           cls.ItemName.label('ProductName'),cls.ShareQty,
                           ItemBOM.CostCenterCode,ItemBOM.ItemCode,
                           ItemBOM.OtherName,ItemBOM.Qty,ItemBOM.PurchasePolicy,cls.CreateUser,cls.CreateTime)

        product = MyProcess(pd.read_sql,sql.statement,cls.getBind())
        pricelist = MyProcess(PriceList.list,division, costCenterCode, date, 'Food', False)
        product = product.get()
        pricelist = pricelist.get()

        if product.empty or pricelist.empty:
            Error(lang('D08CA9F5-3BA5-4DE6-9FF8-8822E5ABA1FF'))  # No data

        # PriceList
        pricelist['PurBOMConversion'] = pricelist['PurStkConversion'] * pricelist['StkBOMConversion']
        pricelist.drop(['PurStkConversion', 'StkBOMConversion','StockUnit'], axis=1, inplace=True)
        df = merge(product,pricelist, how='outer',left_on='ItemCode',right_on='ItemCode')
        DataFrameSetNan(df)
        df['ItemType'] = df['ProductCode'].map(lambda x: 'FG' if x else 'RM')

        # Product -> BOM
        def _product(df):
            groupbyFields = ['CategoriesClassGuid','CookwayClassGuid','ItemShapeGuid','ProductGuid',
                             'ProductCode','ProductName','ShareQty','CreateUser','CreateTime','ItemType']

            return df.groupby(by=groupbyFields)\
                .apply(lambda x: pd.Series({'ItemCost': (x['Price']*x['Qty']/x['PurBOMConversion']).sum(),
                                            'ItemBOM': [reduce(lambda x1, x2: x1 + x2,
                                                               [[{k: v for k, v in l.items()
                                                                  if k not in groupbyFields and v}]
                                                                for l in x.sort_values(by=['ItemCode'])
                                                               .to_dict('records')])]}))\
                .reset_index().rename(columns={'ProductGuid':'ItemGuid',
                                               'ProductCode':'ItemCode',
                                               'ProductName':'ItemName'})

        product = MyProcess(_product,df[df['ItemType']=='FG'])

        # Product category
        itemClass =ItemClass.list(2)
        rms = df.loc[df['ItemType'] == 'RM',
                     ['ClassCode','ClassName','ItemCode','ItemName','PurUnit','Price','ItemType']]\
            .rename(columns={'PurUnit':'ItemUnit','Price':'ItemCost',
                             'ClassCode':'CategoriesClassGuid','ClassName':'CategoriesClassName'})

        product = product.get()
        product['ItemUnit'] = 'pc'
        for l in ['CategoriesClass','CookwayClass','ItemShape']:
            product = merge(product,itemClass,how='left',left_on=(l+'Guid'),right_on='guid')\
                .drop(['guid'],axis=1)\
                .rename(columns={'ClassName':l+'Name','Sort':l+'Sort'})

        df = product.append(rms)
        DataFrameSetNan(df)
        return getdict(df.sort_values(by=['ItemType','CategoriesClassSort','ItemCode']))