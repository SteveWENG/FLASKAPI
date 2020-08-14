from flask import Blueprint
from flask_restful import Api

# 定义蓝图，main为蓝图名字
api_bp = Blueprint('wechat', __name__, url_prefix='/wechat')

# 实例化api
api = Api(api_bp)

from .. import wechat