# -*- coding: utf-8 -*-

from flask import Blueprint

from ..BLL.OrderHelper import OrderHelper
from ..api import webapi

bp = Blueprint('order', __name__, url_prefix='/order')

@bp.route('/dates', methods=['POST'])
def dates():
    return webapi(lambda dic: OrderHelper.dates(dic))

@bp.route('/updatepo', methods=['POST'])
def updatepo():
    return webapi(lambda dic: OrderHelper.updatepo(dic))

@bp.route('/save', methods=['POST'])
def save():
    return webapi(lambda dic: OrderHelper.save(dic))