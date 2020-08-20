from flask import Flask
from flask_cors import CORS
from werkzeug.utils import import_string

from .entity import db
from .config import Config

blueprints = ['wechat', 'sfeed', 'stock','user']

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

    return app