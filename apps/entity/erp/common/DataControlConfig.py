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
        tmp = cls.query.filter(cls.Type=='StockReport',func.coalesce(cls.Val1,'').like('%'+type+'%'),
                               func.coalesce(cls.StartDate,'2000-1-1')<=today,
                               func.coalesce(cls.EndDate,'2222-12-31')>=today)\
            .with_entities(cls.Val2,cls.Val3,cls.Val4,cls.Val5,cls.Val6,cls.Val7)\
            .order_by(cls.SortName)

        tmp = pd.read_sql(tmp.statement,cls.getBind())
        if tmp.empty:
            Error(lang('D08CA9F5-3BA5-4DE6-9FF8-8822E5ABA1FF'))

        ret = {}

        # Openning
        qry = tmp[tmp['Val2']== 'OpenningOfReportParameter']
        if not qry.empty:
            dic = {}
            s = qry.iloc[0]['Val3']
            if s: dic['value'] = s=='true'

            s = qry.iloc[0]['Val4']
            if s: dic['show'] = True

            if dic: ret['openning'] = dic

        # columns
        qry = tmp[tmp['Val2'] != 'OpenningOfReportParameter']
        if qry.empty: return ret

        qry.rename(columns={'Val2':'value','Val3':'label','Val4':'children',
                            'Val5':'group','Val6':'width'},inplace=True)
        qry.loc[~qry['Val7'].isna(),'checked'] = qry['Val7'] == 'true'
        qry.drop(['Val7'],axis=1,inplace=True)

        if not qry[qry.apply(lambda x: ';' in x['value'],axis=1)].empty:
            qry['values'] = qry.apply(lambda x: [eval(l) for l in x['value'].split(';')]
                                                if ';' in getStr(x['value']) else '', axis=1)
            qry['value'] = qry.apply(lambda x: ''  if ';' in getStr(x['value']) else x['value'], axis=1)

        DataFrameSetNan(qry)
        ret['columns'] = getdict(qry)

        return ret