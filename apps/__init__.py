import logging

from flask import Flask, current_app
from flask_cors import CORS
from werkzeug.utils import import_string

from .Log.dbHandler import dbHandler
from .entity import db
from config import Config

blueprints = ['wechat', 'sfeed', 'stock','user','order','common']

# 函数工厂
def create_app():
    # 初始化flask
    app = Flask(__name__)
    cors = CORS(app)

    # 从对象设置配置信息
    app.config.from_object(Config)

    # 第三方扩展初始化
    db.init_app(app)

    # 注册蓝图    
    for bp_name in blueprints:
        bp_name = 'apps.api.%s:bp' %bp_name
        bp = import_string(bp_name)
        app.register_blueprint(bp)

    create_log(app)
    init_global()
    return app

def create_log(app):
    handler = dbHandler()
    handler.setLevel(20)
    app.logger.addHandler(handler)
    app.logger.setLevel(20)

    if 'SQLALCHEMY_ECHO' not in Config.__dict__ or Config.SQLALCHEMY_ECHO != True:
        return

    def _filter(record):
        # log表
        if 'sqlalchemy.' not in record.name: return True
        # db log
        if record.name == 'sqlalchemy.engine.base.Engine.adenlog' \
                and record.module == 'base':
            return False
        if record.levelno > 20: return True

        # if 'FROM [INFORMATION_SCHEMA].[' in record.message: return False

        return record.message.lower() not in ['{}', 'select @@version', 'select schema_name()',
                                              "select cast('test max support' as nvarchar(max))",
                                              "select cast('test unicode returns' as nvarchar(60)) as anon_1",
                                              "select cast('test plain returns' as varchar(60)) as anon_1"]

    nologfilter = logging.Filter()
    nologfilter.filter = lambda record: _filter(record)
    handler.addFilter(nologfilter)

    logger = logging.getLogger('sqlalchemy.engine')
    logger.addHandler(handler)
    logger.setLevel(20)

def init_global():
    # 初始化一个全局的字典
    global _global_dict
    _global_dict = {}

def remove_key(key):
    if key not in _global_dict:
        return
    del _global_dict[key]

def set_value(key, value):
    _global_dict[key] = value

def get_value(key):
    try:
        return _global_dict[key]
    except Exception as e:
        return None