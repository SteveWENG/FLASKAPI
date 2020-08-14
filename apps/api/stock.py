# -*- coding: utf-8 -*-

from flask import Blueprint

from ..BLL.StockHelper import StockHelper
from ..api import webapi

bp = Blueprint('stock', __name__, url_prefix='/stock')

@bp.route('/save', methods=['POST'])
def save():
    return webapi(lambda dic: StockHelper.save(dic))