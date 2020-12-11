# -*- coding: utf-8 -*-
from sqlalchemy import func

from .LangMast import lang
from ....utils.functions import *
from ...erp import erp, db

class AppHelp(erp):
    __tablename__ = 'tblAppHelps'

    AppGuid = db.Column()
    Description = db.Column()
    ImgPath = db.Column()
    SortName = db.Column()
    StartDate = db.Column(db.Date)
    EndDate = db.Column(db.Date)

    @classmethod
    def list(cls, appGuid):
        if not appGuid:
            Error(lang('D08CA9F5-3BA5-4DE6-9FF8-8822E5ABA1FF'));

        today = datetime.date.today()
        tmp = cls.query.filter(cls.AppGuid==appGuid,
                               func.coalesce(cls.StartDate,'2000-1-1')<=today,
                               func.coalesce(cls.EndDate,'2222-12-31')>=today)\
            .order_by(cls.SortName)\
            .with_entities(cls.Description,cls.ImgPath).all();
        return getdict(tmp)