# -*- coding: utf-8 -*-

from flask import Blueprint

from ..BLL.OrderHelper import OrderHelper
from ..api import webapi

bp = Blueprint('order', __name__, url_prefix='/order')

@bp.route('/save', methods=['POST'])
def save():
    return webapi(lambda dic: OrderHelper.save(dic))