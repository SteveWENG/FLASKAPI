# -*- coding: utf-8 -*-

from flask import Blueprint

from ..BLL.RecipeHelper import RecipeHelper
from ..BLL.ItemHelper import ItemHelper
from ..api import webapi

bp = Blueprint('item', __name__, url_prefix='/item')

@bp.route('/pricelist', methods=['POST'])
def PriceList():
    return webapi(lambda dic: ItemHelper.PriceList(dic))

@bp.route('/recipes',methods=['POST'])
def recipes():
    return webapi(lambda dic: RecipeHelper.list(dic))

@bp.route('/save',methods=['POST'])
def save():
    return webapi(lambda dic: RecipeHelper.save(dic))