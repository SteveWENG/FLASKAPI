from apps.entity.wechat import wechat, db

class WechatConfig(wechat):
    """description of class"""
    __tablename__ = 'tblWechatConfig'

    AppName = db.Column(db.String)
    AppId = db.Column(db.String)
    AppSecret = db.Column(db.String)