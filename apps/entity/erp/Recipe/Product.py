# -*- coding: utf-8 -*-
import threading
from functools import reduce

import pandas as pd
from flask import current_app
from pandas import merge
from sqlalchemy import func, and_
from sqlalchemy.ext.hybrid import hybrid_property

from ..Item.DivisionItem import DivisionItem
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
    def list(cls,division, costCenterCode, date,fortype):
        if not division and not costCenterCode:
            Error(lang('D08CA9F5-3BA5-4DE6-9FF8-8822E5ABA1FF'))  # No data

        if division:
            if not costCenterCode: fortype = 'recipe'
        else:
            division = CostCenter.GetDivision(costCenterCode)

        filters = [cls.Division==division, cls.Status=='active',ItemBOM.DeleteTime==None,
                   ~cls.BOMs.any(ItemBOM.ItemCode.like('[AB]%'))]
        fields = [cls.CategoriesClassGuid,cls.CookwayClassGuid,cls.ItemShapeGuid,
                  cls.Guid.label('ProductGuid'),cls.ItemCode.label('ProductCode'),
                  cls.ItemName.label('ProductName'),cls.ShareQty,
                  ItemBOM.CostCenterCode,ItemBOM.ItemCode,
                  ItemBOM.OtherName,ItemBOM.Qty,ItemBOM.PurchasePolicy,cls.CreateUser,cls.CreateTime]
        if costCenterCode:
            filters.append(func.coalesce(ItemBOM.CostCenterCode,costCenterCode)==costCenterCode)
        # BOM
        sql = cls.query.filter(*filters).join(ItemBOM,cls.Guid==ItemBOM.ProductGuid)
        if fortype == 'recipe':
            sql = sql.outerjoin(DivisionItem,and_(ItemBOM.ItemCode==DivisionItem.ItemCode,
                                                DivisionItem.Division==division))
            fields += [ItemBOM.Id,DivisionItem.PurPrice,DivisionItem.BOMUnit.label('BOMUnit'),
                       DivisionItem.PurBOMConversion.label('PurBOMConversion')]
        sql = sql.with_entities(*fields)

        product = MyProcess(pd.read_sql,sql.statement,cls.getBind())
        if fortype == 'menu':
            pricelist = MyProcess(PriceList.list,division, costCenterCode, date, 'Food', False)
            pricelist = pricelist.get()
            if pricelist.empty:
                Error(lang('D08CA9F5-3BA5-4DE6-9FF8-8822E5ABA1FF'))  # No data

            # PriceList
            pricelist['PurBOMConversion'] = pricelist['PurStkConversion'] * pricelist['StkBOMConversion']
            pricelist.drop(['PurStkConversion', 'StkBOMConversion','StockUnit'], axis=1, inplace=True)

        product = product.get()
        if product.empty:
            Error(lang('D08CA9F5-3BA5-4DE6-9FF8-8822E5ABA1FF'))  # No data

        if fortype == 'recipe':
            product.rename(columns={'PurPrice':'Price'},inplace=True)
        else:
            df = merge(product,pricelist, how='outer',left_on='ItemCode',right_on='ItemCode')
            DataFrameSetNan(df)

            df['ItemType'] = df['ProductCode'].map(lambda x: 'FG' if x else 'RM')
            product = df[df['ItemType']=='FG']

        # Product -> BOM
        groupbyFields = ['CategoriesClassGuid', 'CookwayClassGuid', 'ItemShapeGuid', 'ProductGuid',
                         'ProductCode', 'ProductName', 'ShareQty', 'CreateUser', 'CreateTime', 'ItemType']
        product = product.groupby(by=list(set(groupbyFields).intersection(set(product.columns))))\
            .apply(lambda x: pd.Series({'ItemCost': (x['Price']*x['Qty']/x['PurBOMConversion']).sum(),
                                        'ItemBOM': [reduce(lambda x1, x2: x1 + x2,
                                                           [[{k: v for k, v in l.items()
                                                              if k not in groupbyFields and v}]
                                                            for l in x.sort_values(by=['ItemCode'])
                                                           .to_dict('records')])]}))\
            .reset_index().rename(columns={'ProductGuid':'ItemGuid',
                                           'ProductCode':'ItemCode',
                                           'ProductName':'ItemName'})
        product['ItemUnit'] = 'pc'

        # Product category
        itemClass =ItemClass.list(2)
        for l in ['CategoriesClass','CookwayClass','ItemShape']:
            product = merge(product,itemClass,how='left',left_on=(l+'Guid'),right_on='guid')\
                .drop(['guid'],axis=1)\
                .rename(columns={'ClassName':l+'Name','Sort':l+'Sort'})

        if fortype == 'menu':
            rms = df.loc[df['ItemType'] == 'RM',
                         ['ClassCode', 'ClassName', 'ItemCode', 'ItemName', 'PurUnit', 'Price', 'ItemType']] \
                .rename(columns={'PurUnit': 'ItemUnit', 'Price': 'ItemCost',
                                 'ClassCode': 'CategoriesClassGuid', 'ClassName': 'CategoriesClassName'})
            product = product.append(rms)

        DataFrameSetNan(product)
        ttt = [t for t in ['ItemType','CategoriesClassSort', 'ItemCode'] if t in set(product.columns)]
        return getdict(product.sort_values(by=ttt))