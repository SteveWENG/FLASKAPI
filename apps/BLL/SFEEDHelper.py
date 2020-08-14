from operator import attrgetter

from sqlalchemy import and_, desc
import pandas as pd

from ..entity import SaveDB, log
from ..entity.sfeed.Item import Item
from ..entity.sfeed.SOLine import SOLine
from ..entity.sfeed.Site import Site
from ..utils.functions import *
from ..entity.sfeed.User import User
from ..entity.sfeed.SOHead import SOHead

class SFEEDHelper:
    
    @classmethod
    @log
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
        if getStr(SiteGuid) == '':
            Error('Not found site guid')
        if getStr(OrderCode) == '':
            Error('没有找到该订单')
          
        try:
            '''
            so  = SOHead.query.filter(and_(SOHead.SiteGUID == SiteGuid, SOHead.OrderCode == OrderCode,
                                           SOHead.Status == 20, SOHead.IsPaid))\
                      .options(load_only('Id','RequiredDate','UserId','GUID'),
                               subqueryload('user').load_only('FirstName'),
                               subqueryload('lines').options(load_only('ItemGUID','Qty'),
                                                             subqueryload('item').load_only('ItemName'))).first()
            '''
            so = SOHead.query.filter(SOHead.SiteGUID == SiteGuid, SOHead.OrderCode == OrderCode,
                                     SOHead.IsPaid, SOHead.Status.in_((10,20)))\
                .join(SOLine, SOHead.GUID==SOLine.SOGUID).join(Item, SOLine.ItemGUID==Item.GUID)\
                .join(User, SOHead.UserId==User.Id)\
                .with_entities(SOHead.Id, SOHead.RequiredDate, SOHead.ShippedDate,
                               User.FirstName, Item.ItemName, SOLine.Qty, SOLine.Id.label('SOLineId')).all()

            if so == None or len(so) == 0:
                Error('无效单号')
            if type == 'ShipList' and so[0].ShippedDate != None:
                Error('该订单已发货，请重新确认订单号')

            groupbyfields = ['OrderId','UserName', 'OrderDate']
            so = pd.DataFrame(so).rename(columns={'Id': 'OrderId',
                                                  'FirstName':'UserName',
                                                  'RequiredDate':'OrderDate'})
            so['OrderDate'] = so['OrderDate'].map(lambda x: x.strftime('%Y-%m-%d'))
            so['OrderId'] = so['OrderId'].astype(str)

            so = so.groupby(groupbyfields, as_index=False)

            tmp = [{**dict(zip(groupbyfields,keys)),#lines[groupbyfields].iloc[:1,:].to_dict('records')[0],
                    **{'Lines': lines.sort_values(by='SOLineId', axis=0)[['ItemName','Qty']]
                        .rename(columns={'Qty':'qty'}).to_dict('records')}}
                   for keys,lines in so][0]

            return tmp
            # return Enumerable(so).group_by(key_names=['Id1','FirstName1','RequiredDate1'],
            #                                key=lambda x: [x.Id, x.FirstName, x.RequiredDate.strftime('%Y-%m-%d')],
            #                                result_func=lambda x: {'OrderId' : x.key.Id1, 'UserName' : x.key.FirstName1,
            #                                                       'OrderDate' : x.key.RequiredDate1,
            #                                                       'Lines' : [{'ItemName': l.ItemName, 'qty': l.Qty}
            #                                                                  for l in x.order_by(lambda y: y.SOLineId).to_list()]})\
            #     .first_or_default()

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

