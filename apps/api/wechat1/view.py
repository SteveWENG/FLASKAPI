# -*- coding: utf-8 -*-

from flask_restful import Resource

from ..wechat import api
from apps.api import webapi
from apps.BLL.WechatHelper import WechatHelper


@api.resource('/CodeURL')
class CodeURL(Resource):
    def get(self):
        return 'welcome 上海'

    def post(self):
        return webapi(lambda dic: WechatHelper(dic.get('AppName', '')).getCodeUrl(dic.get('RedirectUrl')))


@api.resource('/UserInfo')
class UserInfo(Resource):
    def post(self):
        return webapi(lambda dic: WechatHelper(dic.get('AppName', '')).GetUserInfo(dic.get('code')))


@api.resource('/JSApiTicket')
class JSApiTicket(Resource):
    def post(self):
        return webapi(lambda dic: WechatHelper(dic.get('AppName', '')).GetJSApiTicket())