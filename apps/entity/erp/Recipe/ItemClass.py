# -*- coding: utf-8 -*-

import math
import pandas as pd
from pandas import merge
from sqlalchemy.ext.hybrid import hybrid_property

from apps.utils.functions import DataFrameSetNan
from ...erp import erp, db

class ItemClass(erp):
    __tablename__ = 'tblItemClass'

    guid = db.Column()
    pguid = db.Column()
    Status = db.Column()

    @hybrid_property
    def ClassName(self):
        return self.LangColumn('ClassName_')

    @classmethod
    def list(cls, classNameNbr=None):
        if classNameNbr: classNameNbr -= 1
        sql = cls.query.filter(cls.Status=='active')\
            .with_entities(cls.pguid,cls.guid,cls.ClassName.label('ClassName'))
        df = pd.read_sql(sql.statement,cls.getBind())
        DataFrameSetNan(df)
        # 从子开始
        tdf = df[df.apply(lambda x: x['guid'] not in list(df['pguid']),axis=1)]
        df.rename(columns={'guid': 'tguid', 'ClassName': 'tClassName','pguid':'tpguid'}, inplace=True)
        while not tdf[(~tdf['pguid'].isna())&(tdf['pguid']!='')].empty:
            tdf = merge(tdf,df, how='left',left_on='pguid',right_on='tguid').drop('pguid',axis=1)
            tdf.rename(columns={'tpguid':'pguid'},inplace=True)
            tdf.loc[(~tdf['tguid'].isna())&(tdf['tguid']!=''),'tparent'] = tdf.apply(lambda x: [{'guid':x['tguid'],'ClassName':x['tClassName']}],axis=1)
            if 'parent' in tdf.columns:
                tdf['parent'] = tdf['parent'] + tdf['tparent']
            else:
                tdf['parent'] = tdf['tparent']
            tdf.drop(['tguid','tClassName','tparent'],axis=1,inplace=True)

        tdf.drop(['pguid'],axis=1,inplace=True)
        DataFrameSetNan(tdf)
        tdf['ClassName'] = tdf.apply(lambda x:
                                     '/'.join([l['ClassName']
                                               for l in x['parent'][::-1][0:classNameNbr
                                         if classNameNbr else len(x['parent'])]]+[x['ClassName']]),
                                     axis=1)

        return tdf

