from ..sfeed import sfeed, db
from .SOLine import SOLine
from .Item import Item

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
