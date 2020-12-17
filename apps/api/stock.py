# -*- coding: utf-8 -*-

from flask import Blueprint

from ..BLL.StockHelper import StockHelper
from ..api import webapi

bp = Blueprint('stock', __name__, url_prefix='/stock')

@bp.route('/items', methods=['POST'])
def items():
    return webapi(lambda dic: StockHelper.items(dic))

@bp.route('/save', methods=['POST'])
def save():
    return webapi(lambda dic: StockHelper.save(dic))

@bp.route('/UpdateOpenningStock', methods=['POST'])
def UpateOpenningStock():
    return webapi(lambda dic: StockHelper.UpdateOpenningStock(dic))

@bp.route('/list', methods=['POST'])
def list():
    return webapi(lambda dic: StockHelper.list(dic))

@bp.route('/columns', methods=['POST'])
def columns():
    return webapi(lambda dic: StockHelper.columns(dic))