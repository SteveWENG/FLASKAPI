from .. import BaseModel, db, SaveDB

class sfeed(BaseModel):
    __abstract__  = True

    __bind_key__  = 'sfeed'