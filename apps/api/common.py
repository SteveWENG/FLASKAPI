# -*- coding: utf-8 -*-

from flask import Blueprint

from ..BLL.CommonHelper import CommonHelper
from ..api import webapi

bp = Blueprint('common', __name__, url_prefix='/common')

@bp.route('/apphelp', methods=['POST'])
def dates():
    return webapi(lambda dic: CommonHelper.AppHelp(dic))