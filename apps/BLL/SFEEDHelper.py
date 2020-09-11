
from sqlalchemy import and_, desc

from ..entity import SaveDB, dblog
from ..entity.sfeed.Site import Site
from ..utils.functions import *
from ..entity.sfeed.User import User
from ..entity.sfeed.SOHead import SOHead

class SFEEDHelper:
    
    @classmethod
    def GetLogonUser(cls, OpenId, **kw):
        if getStr(OpenId) == '':
            Error('No open id')
        try:
            # user = User.query.filter(User.WechatID == OpenId)\
            #     .options(load_only('SiteGUID','Id'),subqueryload('site').load_only('Code')).first()
            user = User.query.filter(User.WechatID == OpenId).join(Site, Site.GUID == User.SiteGUID)\
                .with_entities(User.SiteGUID,User.Id, Site.Code).first()
            if user == None:
                Error('Not found this user')

            if user.Code == None:
                Error('No site for this user')

            return {'UserId': user.Id, 'SiteGuid': user.SiteGUID, 'CostCenterCode': user.Code} #user.site.Code}
        except Exception as e:
            raise e

    @classmethod
    def GetSO(cls, SiteGuid, OrderCode, type):
        try:
            return SOHead.SO(SiteGuid, OrderCode,type)
        except Exception as e:
            raise e

    @classmethod
    def Shipment(cls, Id, UserId):
        try:
            if getInt(Id) == 0:
                Error('错误的单号')

            if getInt(UserId) == 0:
                Error('错误的员工号')

            with SaveDB() as session:
                if session.query(SOHead).filter(
                        and_(SOHead.Id == Id, SOHead.Status == 20, SOHead.IsPaid == True, SOHead.ShippedDate == None)) \
                        .update({'ShippedUser': UserId, 'Status': 10, 'ShippedDate': datetime.datetime.now()}) < 1:
                    so = SOHead.query.filter(and_(SOHead.Id == Id)).first()
                    if so == None:
                        Error('无效单号')
                    elif not so.IsPaid:
                        Error('该订单还没有确认，无法发货')
                    elif so.ShippedDate != None:
                        Error('该订单已发货，请重新确认订单号')
            return '成功发货'
        except Exception as e:
            raise e

