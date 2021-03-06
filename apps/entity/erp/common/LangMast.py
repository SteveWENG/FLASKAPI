# -*- coding: utf-8 -*-

from flask import g

from ....utils.functions import Error
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

def lang(guid=None):
    if guid:
        return LangMast.getText(guid)

    return g.get('LangCode','EN').upper()

def getParameters(dic, cols, checkNoEmpty=True):
    if isinstance(cols,str):
        cols = cols.split(',')

    if checkNoEmpty:
        ps = [col for col in cols if not dic.get(col)]
        if ps:
            Error(lang('BE5A9D64-A7D9-4DB8-B399-5C886BD33D9D') % ','.join(ps))

    tmp = [dic.get(col,'') for col in cols]
    if len(tmp) == 1:
        return tmp[0]

    return tmp