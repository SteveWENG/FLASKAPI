from .. import BaseModel,db

class wechat(BaseModel):
    __abstract__  = True

    __bind_key__  = 'salesorder'