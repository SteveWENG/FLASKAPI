# -*- coding: utf-8 -*-

import pandas as pd
from pandas import merge

from ..entity.erp.Recipe.ItemBOM import ItemBOM
from ..entity.erp.Recipe.Product import Product
from ..utils.functions import *



class RecipeHelper:

    @staticmethod
    def list(data):
        return Product.list(data.get('division'),data.get('costCenterCode'),data.get('date'),data.get('type'))

    @staticmethod
    def save(data):
        if data.get('ItemGuid'):
            data['Guid'] = data.pop('ItemGuid')

        # Save BOM to site
        def _BOM(boms):
            costCenterCode = data.get('costCenterCode', '')

            dbbom = ItemBOM.query.filter(ItemBOM.ProductGuid == data['Guid']) \
                .with_entities(ItemBOM.Id, ItemBOM.ItemCode,ItemBOM.CostCenterCode)
            dbbom = pd.read_sql(dbbom.statement, ItemBOM.getBind())
            if dbbom.empty:
                return getdict(boms)
            DataFrameSetNan(dbbom)

            tmp = dbbom[dbbom['CostCenterCode']==costCenterCode]
            if not tmp.empty:
                boms = merge(boms,tmp[['Id','ItemCode']] , how='left', left_on='ItemCode', right_on='ItemCode')
            DataFrameSetNan(boms)
            return getdict(boms) + \
                   getdict(dbbom[dbbom['CostCenterCode']!=costCenterCode][['Id']])

        if 'ItemBOM' in list(data.keys()) and not data['ItemBOM']:
            del data['ItemBOM']
        if data.get('ItemBOM'):
            boms = pd.DataFrame(data['ItemBOM'])
            DataFrameSetNan(boms)
            boms.rename(columns={'PurPrice':'ItemCost','BOMUnit':'UOM','PurBOMConversion':'ConversionRate'},inplace=True)
            boms['CostCenterCode'] = data.get('costCenterCode','')
            if 'Id' in list(boms.columns):
                boms.drop(['Id'], axis=1, inplace=True)

            data['ItemBOM'] = _BOM(boms)

        product = Product(data)

        return product.save()

