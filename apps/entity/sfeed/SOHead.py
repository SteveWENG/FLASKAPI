import pandas as pd

from .User import User
from ..sfeed import sfeed, db
from .SOLine import SOLine
from .Item import Item as SfeedItem
from ...utils.functions import *


class SOHead(sfeed):
    __tablename__ = 'tblSaleOrder'

    Id = db.Column('OrderID',primary_key=True)
    SiteGUID = db.Column(db.String)
    OrderCode = db.Column(db.String)
    GUID = db.Column(db.String)
    RequiredDate = db.Column(db.Date)
    UserId = db.Column(db.String)

    IsPaid = db.Column(db.Boolean)
    Status = db.Column(db.Integer)
    ShippedDate = db.Column(db.DateTime)
    ShippedUser = db.Column(db.Integer)


    user = db.relationship('User', foreign_keys=[UserId], primaryjoin='SOHead.UserId == User.Id', lazy='joined')
    lines = db.relationship('SOLine', primaryjoin='SOHead.GUID == foreign(SOLine.SOGUID)', lazy='joined')

    @classmethod
    def SO(cls, SiteGuid, OrderCode, type):
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
                                     SOHead.IsPaid, SOHead.Status.in_((10, 20))) \
                .join(SOLine, SOHead.GUID == SOLine.SOGUID).join(SfeedItem, SOLine.ItemGUID == SfeedItem.GUID) \
                .join(User, SOHead.UserId == User.Id) \
                .with_entities(SOHead.Id.label('OrderId'), SOHead.RequiredDate.label('OrderDate'), SOHead.ShippedDate,
                               User.FirstName.label('UserName'), SfeedItem.ItemName, SOLine.Qty,
                               SOLine.Id.label('SOLineId'))

            so = pd.read_sql(so.statement, cls.getBind())
            if so.empty:
                Error('无效单号')
            if type == 'ShipList' and so.loc[0,'ShippedDate']:
                Error('该订单已发货，请重新确认订单号')

            groupbyfields = ['OrderId', 'UserName', 'OrderDate']
            # so['OrderDate'] = so['OrderDate'].map(lambda x: x.strftime('%Y-%m-%d'))
            so['OrderId'] = so['OrderId'].astype(str)

            so = so.groupby(groupbyfields, as_index=False)

            tmp = [{**dict(zip(groupbyfields, keys)),  # lines[groupbyfields].iloc[:1,:].to_dict('records')[0],
                    **{'Lines': lines.sort_values(by='SOLineId', axis=0)[['ItemName', 'Qty']]
                        .rename(columns={'Qty': 'qty'}).to_dict('records')}}
                   for keys, lines in so][0]

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