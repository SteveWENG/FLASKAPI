from ..sfeed import sfeed, db

class SOLine(sfeed):
    __tablename__ = 'tblSaleOrderItem'

    SOGUID = db.Column(db.String)
    ItemGUID = db.Column(db.String)
    Qty = db.Column(db.Numeric)

    item = db.relationship('Item', foreign_keys=[ItemGUID], primaryjoin='SOLine.ItemGUID == Item.GUID', lazy='joined')