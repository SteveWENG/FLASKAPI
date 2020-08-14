from ..sfeed import db, sfeed

class Site(sfeed):
    __tablename__= 'tblSite'

    GUID = db.Column(db.String)
    Code = db.Column(db.String)
