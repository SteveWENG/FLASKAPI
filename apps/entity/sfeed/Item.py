from ..sfeed import sfeed, db

class Item(sfeed):
    __tablename__ = 'tblItem'

    Id = db.Column('ItemID',db.Integer, primary_key=True)
    GUID = db.Column(db.String)
    ItemName = db.Column(db.String)
