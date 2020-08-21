# -*- coding: utf-8 -*-

from flask import Blueprint

from ..BLL.UserHelper import UserHelper
from ..api import webapi

bp = Blueprint('user', __name__, url_prefix='/user')

@bp.route('/login', methods=['POST'])
def login():
    return webapi(lambda dic: UserHelper.Rights(dic))

@bp.route('/appdata', methods=['POST'])
def appdata():
    return webapi(lambda dic: UserHelper.AppData(dic))