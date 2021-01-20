# -*- coding: utf-8 -*-

import pandas as pd
from pandas import merge

from apps.utils.functions import DataFrameSetNan
from ...erp import erp, db

class ItemClass(erp):
    __tablename__ = 'tblItemClass'

    guid = db.Column()
    pguid = db.Column()
    ClassName = db.Column('ClassName_ZH')
    Status = db.Column()

    @classmethod
    def list(cls):
        sql = cls.query.filter(cls.Status=='active')\
            .with_entities(cls.pguid,cls.guid,cls.ClassName.label('ClassName'))
        df = pd.read_sql(sql.statement,cls.getBind())
        DataFrameSetNan(df)
        tdf = df.loc[df['pguid']=='',['guid','ClassName']]
        x = 0
        while len(tdf)>len(tdf[tdf['guid'].isna()]):
            x = x + 1
            sx = str(100+x)[1:]
            tdf.rename(columns={'guid': 'guid'+sx, 'ClassName': 'ClassName'+sx}, inplace=True)
            tdf = merge(tdf,df,how='left',left_on='guid'+sx,right_on='pguid').drop('pguid',axis=1)

        tdf.drop(['guid','ClassName'],axis=1,inplace=True)
        DataFrameSetNan(tdf)
        return tdf

