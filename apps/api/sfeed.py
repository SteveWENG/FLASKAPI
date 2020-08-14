from flask import Blueprint

from ..api import webapi
from ..BLL.SFEEDHelper import SFEEDHelper

bp = Blueprint('sfeed', __name__, url_prefix='/sfeed')

@bp.route('/', methods=['GET'])
def show():
    return 'welcome 上海'

@bp.route('/LoginUser', methods=['POST'])
def LoginUser():
    return webapi(lambda dic: SFEEDHelper.GetLogonUser(dic.get('OpenId'),
                                                       LogCreatedUser = dic.get('User'),
                                                       LogDBName1 = 'china',
                                                       LogTableName = '上海'))

@bp.route('/ShipList', methods=['POST'])
def ShipList():
    return webapi(lambda dic: SFEEDHelper.GetSO(dic.get('SiteGuid'),dic.get('OrderCode'),'ShipList'))

@bp.route('/Shipment', methods=['POST'])
def Shipment():
    return webapi(lambda dic: SFEEDHelper.Shipment(dic.get('OrderId'),dic.get('UserId')))