from flask import Blueprint

from ..api import webapi
from ..BLL.WechatHelper import WechatHelper

bp = Blueprint('wechat', __name__, url_prefix='/wechat')

@bp.route('/CodeURL', methods=['POST'])
def CodeURL():
    return webapi(lambda dic: WechatHelper(dic.get('AppName','')).getCodeUrl(dic.get('RedirectUrl')))
        
@bp.route('/UserInfo', methods=['POST'])
def UserInfo():
    return webapi(lambda dic: WechatHelper(dic.get('AppName','')).GetUserInfo(dic.get('code')))

@bp.route('/JSApiTicket', methods=['POST'])
def JSApiTicket():
    return webapi(lambda dic: WechatHelper(dic.get('AppName','')).GetJSApiTicket())