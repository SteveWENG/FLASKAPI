# -*- coding: utf-8 -*-

import math
from functools import reduce

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
    Sort = db.Column()
    Type = db.Column()

    @hybrid_property
    def ClassName(self):
        return self.LangColumn('ClassName_')

    @classmethod
    def list(cls, classNameNbr=None):
        if classNameNbr: classNameNbr -= 1
        sql = cls.query.filter(cls.Status=='active')\
            .with_entities(cls.pguid,cls.guid,cls.ClassName.label('ClassName'),cls.Type,cls.Sort)
        df = pd.read_sql(sql.statement,cls.getBind())
        df.loc[df['Type'].str.lower().isin(['cookwayclass','itemshape']),'pguid'] = ''
        DataFrameSetNan(df)
        df['Sort'] = df['Sort'].str.zfill(4)

        x = 0
        tdf = df[['guid','ClassName','Sort']].rename(columns={'ClassName':'pClassName','Sort':'pSort'})
        tdf['pguid'] = tdf['guid']
        tdf['level'] = x
        df.rename(columns={'pguid':'tpguid','guid':'tguid'},inplace=True)

        while ('tguid' not in tdf.columns) or not tdf[~tdf['tguid'].isna()].empty:
            if 'tguid' in tdf.columns:
                x = x + 1
                tdf.loc[~tdf['tguid'].isna(), 'guid'] = tdf['tguid']
                tdf.loc[~tdf['tguid'].isna(), 'level'] = x
            tdf = merge(tdf[['level','pguid','pClassName','pSort','guid']],df,
                        how='left',left_on='guid',right_on='tpguid')

        DataFrameSetNan(tdf)
        def _apply(g):
            g = g.sort_values(by='level',ascending=False)
            return pd.Series({'ClassName': '/'.join(g['pClassName'][:(classNameNbr+1 if classNameNbr else len(g))]),
                              'Sort': ''.join(g['pSort'])})

        tdf = tdf.groupby(by=['guid']).apply(_apply).reset_index()

        return tdf

