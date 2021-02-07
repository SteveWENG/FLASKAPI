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
            # 存division的BOM
            if not boms:
                return None

            costCenterCode = data.get('costCenterCode','')
            dbbom = ItemBOM.query.filter(ItemBOM.ProductGuid == data['Guid']) \
                .with_entities(ItemBOM.Id, ItemBOM.ItemCode,ItemBOM.CostCenterCode)
            dbbom = pd.read_sql(dbbom.statement, ItemBOM.getBind())
            if dbbom.empty:
                return boms
            DataFrameSetNan(dbbom)
            boms = pd.DataFrame(boms)
            boms['CostCenterCode'] = costCenterCode
            boms = merge(boms, dbbom[dbbom['CostCenterCode']==costCenterCode], how='left',
                         left_on='ItemCode', right_on='ItemCode')
            DataFrameSetNan(boms)
            return getdict(boms) \
                   + getdict(dbbom[dbbom['CostCenterCode']!=costCenterCode][['Id']])

        if data.get('ItemBOM'):
            def _process(bom):
                col = {'PurPrice':'ItemCost','BOMUnit':'UOM','PurBOMConversion':'ConversionRate'}
                for k,v in col.items():
                    if bom.get(k): bom[v] = bom.pop(k)

                # Division的BOM，或Site的BOM
                if not data.get('costCenterCode') or bom.get('CostCenterCode'):
                    return bom

                # Menu时存Site的BOM
                del bom['Id']
                return bom
            data['ItemBOM'] = _BOM([_process(bom) for bom in data['ItemBOM']])
            if not data['ItemBOM']:
                del data['ItemBOM']

        product = Product(data)

        return product.save()

