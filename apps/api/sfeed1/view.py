from flask_restful import Resource

from ..sfeed import api
from apps.api import webapi
from apps.BLL.SFEEDHelper import SFEEDHelper

@api.resource('/LoginUser')
class LoginUser(Resource):
    def post(self):
        return webapi(lambda dic: SFEEDHelper.GetLogonUser(dic.get('OpenId')))

@api.resource('/ShipList')
class ShipList(Resource):
    def post(self):
        return webapi(lambda dic: SFEEDHelper.GetSO(dic.get('SiteGuid'),dic.get('OrderCode'),type(self).__name__))

@api.resource('/Shipment')
class Shipment(Resource):
    def post(self):
        return webapi(lambda dic: SFEEDHelper.Shipment(dic.get('OrderId'),dic.get('UserId')))