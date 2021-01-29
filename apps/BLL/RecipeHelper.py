# -*- coding: utf-8 -*-


from apps.entity.erp.Recipe.Product import Product


class RecipeHelper:

    @staticmethod
    def list(data):
        return Product.list(None,data.get('costCenterCode'),data.get('date'),data.get('type'))
