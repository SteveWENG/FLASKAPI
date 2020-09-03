import json, datetime

from ..entity.wechat.WechatConfig import WechatConfig
from ..utils.http import *

class WechatHelper:

    jsapi_ticket = {
            'ticket': '',
            'CreatedTime': None
        }

    def __init__(self, AppName = None):
        if getStr(AppName) == '':
            Error('No parameter of AppName')

        self._wechatConfig = WechatConfig.query.filter(WechatConfig.AppName == AppName).first()

        if self._wechatConfig == None:
            Error('Not found the wechat config')
        
    #RedirectUrl
    def getCodeUrl(self, RedirectUrl = None):
        if getStr(RedirectUrl) == '':
            Error('No paramenters of RedirectUrl')

        url = 'https://open.weixin.qq.com/connect/oauth2/authorize?'
        url += 'appid={0}&redirect_uri={1}&response_type=code&scope=snsapi_userinfo&state=STATE#wechat_redirect'
        url = url.format(self._wechatConfig.AppId, RedirectUrl)
        return url

    def GetUserInfo(self, code):
        if getStr(code) == '':
            Error('微信认证缺少必填参数code！')
        
        try:
            url = 'https://api.weixin.qq.com/sns/userinfo?access_token={0}&openid={1}'
            
            dicOpenId = self._OpenId(code)

            url = url.format(dicOpenId.get('access_token'), dicOpenId.get('openid'))

            dicUser = self._httpget(url)
            dicUser['AppId'] = self._wechatConfig.AppId

            return dicUser
        except Exception as e:
            raise e

    def GetJSApiTicket(self):
        url = 'https://api.weixin.qq.com/cgi-bin/ticket/getticket?access_token={0}&type=jsapi';

        try:
            time = WechatHelper.jsapi_ticket.get('CreatedTime',None)
            if time != None and (datetime.datetime.now() - time).total_seconds() < 7100:
                return WechatHelper.jsapi_ticket.get('ticket','')

            url = url.format(self._AccessToken())
            dicTicket = self._httpget(url)

            WechatHelper.jsapi_ticket['ticket'] = dicTicket.get('ticket')
            WechatHelper.jsapi_ticket['CreatedTime'] = datetime.datetime.now()

            return dicTicket.get('ticket')
        except Exception as e:
            raise e

    def _OpenId(self, code):
        url = 'https://api.weixin.qq.com/sns/oauth2/access_token?appid={0}&secret={1}&code={2}&grant_type=authorization_code'
        url = url.format(self._wechatConfig.AppId, self._wechatConfig.AppSecret,code)

        try:
            return self._httpget(url)
        except Exception as e:
            raise e

    def _AccessToken(self):        
        url = "http://mall.adenchina.net/jc/api/AccessToken.ashx";
        if self._wechatConfig.AppName != 'OfficialAccount':
            Error('Not define AccessToken of ' + self._wechatConfig.AppName)
        try:
            tmp = httpget(url)
            if getStr(tmp) == '':
                Error("Can't get access token")

            return tmp
        except Exception as e:
            raise e

    def _httpget(self, url):
        try:
            restext = httpget(url)

            dictinfo = json.loads(restext)
            if getStr(dictinfo.get('errcode','0')) != '0':
                Error(restext) #显示错误代码和信息

            return dictinfo
        except Exception as e:
            raise e