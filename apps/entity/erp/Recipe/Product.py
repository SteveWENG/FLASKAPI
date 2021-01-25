# -*- coding: utf-8 -*-
import threading
from functools import reduce

import pandas as pd
from pandas import merge
from sqlalchemy import func, and_
from sqlalchemy.ext.hybrid import hybrid_property

from apps.utils.functions import *
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
    CategoriesClassGuid = db.Column()
    CookwayClassGuid = db.Column()
    ItemShapeGuid = db.Column()
    Status = db.Column()

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
                           cls.Guid.label('ProductGuid'),
                           cls.ItemCode.label('ProductCode'),cls.ItemName.label('ProductName'),
                           ItemBOM.CostCenterCode,ItemBOM.ItemCode,
                           ItemBOM.OtherName,ItemBOM.Qty)

        df = pd.read_sql(sql.statement,cls.getBind())
        if df.empty: Error(lang('D08CA9F5-3BA5-4DE6-9FF8-8822E5ABA1FF'))  # No data

        # PriceList
        tmpdf = PriceList.list(division,costCenterCode, date, 'Food', False)
        tmpdf['PurBOMConversion'] = tmpdf['PurStkConversion'] * tmpdf['StkBOMConversion']
        tmpdf.drop(['PurStkConversion', 'StkBOMConversion'], axis=1, inplace=True)
        df = merge(df,tmpdf, how='left',left_on='ItemCode',right_on='ItemCode')
        DataFrameSetNan(df)

        # Product -> BOM
        groupbyFields = ['CategoriesClassGuid','CookwayClassGuid','ItemShapeGuid',
                         'ProductGuid','ProductCode','ProductName']

        df = df.groupby(by=groupbyFields)\
            .apply(lambda x:
                                pd.Series({'ItemBOM':
                                                  [reduce(lambda x1, x2: x1 + x2,
                                                          [[{k: v for k, v in l.items()
                                                             if k not in groupbyFields and v}]
                                                           for l in x.sort_values(by=['ItemCode']).to_dict('records')])]}))\
            .reset_index()

        # Product category
        itemClass = ItemClass.list()
        for l in [['CategoriesClassGuid','CagegoriesClassName'],
                  ['CookwayClassGuid','CookwayClassName'],
                  ['ItemShapeGuid','ItemShapeName']]:
            df = merge(df,itemClass,left_on=l[0],right_on='guid')\
                .drop(['guid','parent'],axis=1)\
                .rename(columns={'ClassName':l[1]})

        return getdict(df.sort_values(by=['CagegoriesClassName','ProductCode']))