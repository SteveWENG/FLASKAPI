# -*- coding: utf-8 -*-

from flask import g
from  ....utils.functions import getStr
from ...erp import erp, db

class LangMast(erp):
    __tablename__ = 'langmast'

    Guid = db.Column()
    Type = db.Column()
    TextZH = db.Column('ZH')
    TextEN = db.Column('EN')
    Status = db.Column(db.Boolean)

    langs = None

    @classmethod
    def getText(cls,guid):
        if not cls.langs:
            cls.langs = LangMast.query.filter(cls.Status == True)\
                .with_entities(cls.Guid, LangMast.TextZH,cls.TextEN).all()

        li = list(filter(lambda x: x.Guid==guid, cls.langs))
        if not li:
            return guid

        return li[0].TextZH if g.get('LangCode','') == 'ZH' else li[0].TextEN