# -*- coding: utf-8 -*-


from apps.entity.erp.Recipe.Product import Product


class RecipeHelper:

    @staticmethod
    def list(data):
        return Product.list(data.get('division'),data.get('costCenterCode'),data.get('date'),data.get('type'))

    @staticmethod
    def save(data):
        if data.get('ItemGuid'):
            data['Guid'] = data.pop('ItemGuid')
        if data.get('ItemBOM'):
            def _process(bom):
                col = {'Price':'ItemCost','BOMUnit':'UOM',}
                for k,v in col.items():
                    if bom.get(k): bom[v] = bom.pop(k)

                # 存成Site的BOM
                if not data.get('costCenterCode') or bom.get('CostCenterCode'):
                    return bom
                del bom['Id']
                bom['CostCenterCode'] = data['costCenterCode']
                return bom
            data['ItemBOM'] = [_process(bom) for bom in data['ItemBOM']]

        return Product(data).save()