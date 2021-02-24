# -*- coding: utf-8 -*-

import pandas as pd
from pandas import merge

from ..entity.erp.Item.PriceList import PriceList
from ..entity.erp.Recipe.ItemBOM import ItemBOM
from ..entity.erp.Recipe.Product import Product
from ..entity.erp.common.CostCenter import CostCenter
from ..entity.erp.common.LangMast import getParameters,lang
from ..utils.MyProcess import MyProcess
from ..utils.functions import *



class RecipeHelper:

    @staticmethod
    def list(data):
        division,costCenterCode,date,type = getParameters(data,'division,costCenterCode,date,type',False)
        type = getStr(type).lower()

        if (not division and not costCenterCode) or (type == 'menu' and not costCenterCode):
            Error(lang('BE5A9D64-A7D9-4DB8-B399-5C886BD33D9D'))

        if division:
            if not costCenterCode: fortype = 'recipe'
        else:
            division = CostCenter.GetDivision(costCenterCode)

        pricelist = None
        if type == 'menu':
            #pricelist = MyProcess(PriceList.list, division, costCenterCode, date, 'Food', False)
            pricelist = PriceList.list(division, costCenterCode, date, 'Food', False)
            if pricelist.empty:
                Error(lang('D08CA9F5-3BA5-4DE6-9FF8-8822E5ABA1FF'))  # No data

            # PriceList
            pricelist['PurBOMConversion'] = pricelist['PurStkConversion'] * pricelist['StkBOMConversion']
            pricelist.drop(['PurStkConversion', 'StkBOMConversion', 'StockUnit'], axis=1, inplace=True)

        product =  Product.list(division,costCenterCode,type,pricelist)

        if not pricelist.empty:
            rms = pricelist[['ClassCode', 'ClassName', 'ItemCode', 'ItemName', 'PurUnit', 'PurPrice']] \
                    .rename(columns={'PurUnit': 'ItemUnit', 'PurPrice': 'ItemCost',
                                     'ClassCode': 'CategoriesClassGuid', 'ClassName': 'CategoriesClassName'})
            product['ItemType'] = 'FG'
            rms['ItemType'] = 'RM'
            product = product.append(rms)

        DataFrameSetNan(product)
        ttt = [t for t in ['ItemType', 'CategoriesClassSort', 'ItemCode'] if t in set(product.columns)]
        return getdict(product.sort_values(by=ttt))

    @staticmethod
    def save(data):
        if data.get('ItemGuid'):
            data['Guid'] = data.pop('ItemGuid')
        if not data.get('Id'):
            data['Guid'] = getGUID()

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
            if not boms.empty and not tmp.empty:
                boms = merge(boms,tmp[['Id','ItemCode']] , how='left', left_on='ItemCode', right_on='ItemCode')
            DataFrameSetNan(boms)

            return getdict(boms) + \
                   getdict(dbbom[dbbom['CostCenterCode']!=costCenterCode][['Id']])

        boms = pd.DataFrame([])
        if data.get('ItemBOM'):
            boms = pd.DataFrame(data['ItemBOM'])
            DataFrameSetNan(boms)
            boms.rename(columns={'PurPrice':'ItemCost','BOMUnit':'UOM','PurBOMConversion':'ConversionRate'},inplace=True)
            boms['CostCenterCode'] = data.get('costCenterCode','')
            if 'Id' in list(boms.columns):
                boms.drop(['Id'], axis=1, inplace=True)

        data['ItemBOM'] = _BOM(boms)
        if not data['ItemBOM']:
            del data['ItemBOM']

        product = Product(data)

        return product.save()

