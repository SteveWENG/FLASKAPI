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
    def WorkDates(cls):
        today = datetime.date.today()
        li = cls.query.filter(cls.StartDate<=(today+datetime.timedelta(days=60)),
                              cls.EndDate>(today+datetime.timedelta(days=-60)))
        li = pd.read_sql(li.statement,cls.getBind())

        def __WorkDates(calendars, step):
            ret = []
            date = datetime.date.today() + datetime.timedelta(days=-1*step)
            while len(ret)<10:
                date += datetime.timedelta(days=step)

                tmp = calendars[(calendars['StartDate']<=date)&(calendars['EndDate']>=date)]
                if not tmp.empty:
                    if tmp.iloc[0]['Working'] == True:
                        ret.append(date)
                    continue

                if date.weekday() > 4: continue

                ret.append(date)
            return ret

        return sorted(set(__WorkDates(li,1) + __WorkDates(li,-1)))