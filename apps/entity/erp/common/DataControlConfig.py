# -*- coding: utf-8 -*-
import datetime
import pandas as pd
from sqlalchemy import func, case

from ... import SaveDB
from ....utils.functions import *
from .LangMast import lang
from ...erp import erp, db


class DataControlConfig(erp):
    __tablename__ = 'tblDataControlConfig'

    Type = db.Column()
    Guid = db.Column()
    Val1 = db.Column()
    Val2 = db.Column()
    Val3 = db.Column()
    Val4 = db.Column()
    Val5 = db.Column()
    Val6 = db.Column()
    Val7 = db.Column()
    SortName = db.Column()
    StartDate = db.Column(db.Date)
    EndDate = db.Column(db.Date)

    @classmethod
    def list(cls,types):
        if not types:
            Error(lang('D08CA9F5-3BA5-4DE6-9FF8-8822E5ABA1FF'))
        filters = [func.coalesce(cls.StartDate,'2000-1-1')<=datetime.date.today(),
                   func.coalesce(cls.EndDate,'2222-12-31')>=datetime.date.today()]
        if isinstance(types,str):
            filters.append(cls.Type==types)
        else:
            filters.append((cls.Type.in_(types)))

        sql = cls.query.filter(*filters)
        return pd.read_sql(sql.statement,cls.getBind())

    @classmethod
    def getPONumber(cls):
        ym = datetime.date.today().strftime('%y%m')
        with SaveDB() as session:
            qry = session.query(cls).filter(func.coalesce(cls.Guid, '')=='0B1ADC15-6217-4FE6-8C9A-A55BA0228BBA')
            qry.update({'Val3': case([(func.coalesce(cls.Val2,'')==ym,
                                       func.coalesce(cls.Val3,0)+1)],
                                     else_=1),
                         'Val2':ym},synchronize_session=False)
            tmp = qry.with_entities(cls.Val3).first().Val3

            return 'PO%s' %(getInt(ym) * 100000 + getInt(tmp))

    @classmethod
    def StockReportCols(cls,data):
        type = data.get('type','detail').lower()
        today = datetime.date.today()
        tmp = cls.query.filter(cls.Type=='StockReport',func.coalesce(cls.Val1,type).like('%'+type+'%'),
                               func.coalesce(cls.StartDate,'2000-1-1')<=today,
                               func.coalesce(cls.EndDate,'2222-12-31')>=today)\
            .with_entities(cls.Val2.label('value'),cls.Val3.label('label'),cls.Val4.label('children'),
                           cls.Val5.label('group'),cls.Val6.label('width'),cls.Val7.label('checked'))\
            .order_by(cls.SortName).all()

        ret = {'columns': [{**{k: getVal(getattr(l, k)).split(',') if k=='children' else
                                    (getVal(getattr(l, k))=='true' if k=='checked' else getVal(getattr(l, k)))
                                for k in l.keys() if getattr(l, k)},
                             **({'sortIndex':'','sortBy':'', 'search':''}
                                if getStr(l.checked)!='' else {})}
                            for l in tmp if l.value != 'OpenningOfReportParameter']}
        tmp = [l for l in tmp if l.value == 'OpenningOfReportParameter']
        if tmp:
            tmp = tmp[0]
            dic = {}
            if tmp.label: # default value
                dic['value'] = tmp.label=='true'
            if tmp.children: # 显示
                dic['show'] = True

            if dic:
                ret['openning'] = dic

        return ret