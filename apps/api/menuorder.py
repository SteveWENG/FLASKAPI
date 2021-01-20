# -*- coding: utf-8 -*-

from flask import Blueprint

from ..BLL.MenuOrderHelper import MenuOrderHelper
from ..BLL.RecipeHelper import RecipeHelper
from ..api import webapi

bp = Blueprint('menuorder', __name__, url_prefix='/menuorder')

@bp.route('/recipes',methods=['POST'])
def recipes():
    return webapi(lambda dic: RecipeHelper.list(dic))

@bp.route('/list',methods=['POST'])
def list():
    return webapi(lambda dic: MenuOrderHelper.list(dic))

@bp.route('/save',methods=['POST'])
def Save():
    return webapi(lambda dic: MenuOrderHelper.Save(dic))