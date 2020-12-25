import logging
import os
from logging import handlers

from flask import Flask
from flask_cors import CORS
from werkzeug.utils import import_string

from .Log.dbHandler import dbHandler
from .entity import db
from config import Config
from .entity.erp.Log import *
from .entity.erp.Log.SQLAlchemyHandler import SQLAlchemyHandler

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

    return app

def create_log(app):
    app.logger.addHandler(dbHandler())
    app.logger.setLevel(20)

    if 'SQLALCHEMY_ECHO' not in Config.__dict__ or Config.SQLALCHEMY_ECHO != True:
        return

    '''
    logpath = './logs'
    if not os.path.exists(logpath):
        os.mkdir(logpath)
    handler = handlers.TimedRotatingFileHandler(os.path.join(logpath, 'app.log'),
                                                when='D', interval=1, backupCount=100, encoding='utf-8')
    # handler = logging.FileHandler('./logs/app.log')
    handler.suffix = "app.%Y%m%d-%H%M.log"
    handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s\n[%(module)s - %(filename)s]\n\t%(message)s"))
    handler.setLevel(20)
    '''

    def _filter(record):
        # log表
        if record.name == 'sqlalchemy.engine.base.Engine.adenlog' \
                and record.module == 'base':
            return False
        if record.levelno > 20: return True

        return record.message.lower() not in ['{}', 'select @@version', 'select schema_name()',
                                              "select cast('test max support' as nvarchar(max))",
                                              "select cast('test unicode returns' as nvarchar(60)) as anon_1",
                                              "select cast('test plain returns' as varchar(60)) as anon_1"]

    handler = dbHandler()
    nologfilter = logging.Filter()
    nologfilter.filter = lambda record: _filter(record)
    handler.addFilter(nologfilter)

    logger = logging.getLogger('sqlalchemy.engine')
    logger.addHandler(handler)
    logger.setLevel(20)