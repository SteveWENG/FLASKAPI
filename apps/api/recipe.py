# -*- coding: utf-8 -*-

# -*- coding: utf-8 -*-

from flask import Blueprint

from ..BLL.RecipeHelper import RecipeHelper
from ..api import webapi

bp = Blueprint('recipe', __name__, url_prefix='/recipe')

@bp.route('/list',methods=['POST'])
def list():
    return webapi(lambda dic: RecipeHelper.list(dic))

@bp.route('/save',methods=['POST'])
def save():
    return webapi(lambda dic: RecipeHelper.save(dic))