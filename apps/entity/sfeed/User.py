from ..sfeed import sfeed, db
from .Site import Site

class User(sfeed):
   __tablename__ = 'tblUser'

   Id = db.Column('UserID', db.Integer, primary_key = True)
   WechatID = db.Column(db.String)
   SiteGUID = db.Column(db.String)
   FirstName = db.Column(db.String)

   site = db.relationship('Site', foreign_keys=[SiteGUID],  primaryjoin='User.SiteGUID == Site.GUID', lazy='joined')