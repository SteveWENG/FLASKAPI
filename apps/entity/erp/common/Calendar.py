# -*- coding: utf-8 -*-

import datetime
import pandas as pd

from ...erp import erp, db


class Calendar(erp):
    __tablename__ = 'tblCalendars'

    Name = db.Column()
    StartDate = db.Column(db.Date)
    EndDate = db.Column(db.Date)
    Working = db.Column()

    @classmethod
    def WorkDates(cls,today=None):
        days = 30
        if not today:
            today = datetime.date.today()
        li = cls.query.filter(cls.StartDate<=(today+datetime.timedelta(days=days)),
                              cls.EndDate>(today+datetime.timedelta(days=-days)))
        li = pd.read_sql(li.statement,cls.getBind())
        dates = []
        for d in [today+datetime.timedelta(days=x) for x in range(-days,days)]:
            tli = li[(li['StartDate']<=d)&(li['EndDate']>=d)]
            if tli.empty:
                if d.weekday() < 5: dates.append(d) # Monday - Friday
                continue

            if tli.iloc[0]['Working'] == True:
                dates.append(d)
        return dates