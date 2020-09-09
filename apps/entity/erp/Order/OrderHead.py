# -*- coding: utf-8 -*-
import math

import pandas as pd

from ..common.LangMast import lang
from ....utils.functions import getGUID, getNumber
from ...erp import erp, db
from .OrderLine import OrderLine
from ....entity import SaveDB, dblog


class OrderHead(erp):
    __tablename__ = 'tblOrderHead'

    HeadGuid = db.Column()
    OrderNo = db.Column()
    OrderDate = db.Column(db.Date)
    CostCenterCode = db.Column()
    CreateUser = db.Column()
    AppGuid = db.Column()
    AppStatus = db.Column()
    FromType = db.Column()

    lines = db.relationship('OrderLine', primaryjoin='OrderHead.HeadGuid == foreign(OrderLine.HeadGuid)',
                            lazy='joined')

    @dblog
    def save(self,lines):
        try:
            dflines = pd.DataFrame(lines)

            # 新增
            if getNumber(self.Id) < 1:
                self.HeadGuid = getGUID()
                self.Id = None
                dflines.drop([f for f in dflines if f.lower()=='id'], axis=1, inplace=True)

            dflines['remainQty'] = dflines['qty']
            self.lines = [OrderLine(l) for l in dflines.to_dict(orient='records')]

            with SaveDB() as session:
                session.merge(self)

            return lang('A16AAA03-DCE8-4936-9D9E-FE23F9AE7378')
        except Exception as e:
            raise e