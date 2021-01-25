# -*- coding: utf-8 -*-

import pandas as pd
from datetime import date

from sqlalchemy.ext.hybrid import hybrid_property,hybrid_method

from apps.utils.functions import *
from ..common.CostCenter import CostCenter
from ..common.DataControlConfig import DataControlConfig
from ..common.LangMast import lang
from ...erp import erp, db

class PriceList(erp):
    __tablename__ = 'DM_D_ERP_PurchaseAgreement_NEW'

    Division = db.Column()
    ItemCode = db.Column()
    ItemNameZH = db.Column('ItemName_ZH')
    ItemNameEN = db.Column('ItemName_EN')
    SupplierCode = db.Column('Supplier_Code')
    SupplierName = db.Column('Supplier_Name')
    Price = db.Column()
    Tax = db.Column()

    PurUnit = db.Column('Pur_Unit')
    StockUnit = db.Column('Stock_Unit')
    BOMUnit = db.Column('BOM_Unit')
    PurStkConversion = db.Column('PurStk_Conversion')
    StkBOMConversion = db.Column('StkBOM_Conversion')
    ValidFrom = db.Column(db.Date)
    ValidTo = db.Column(db.Date)

    '''
    __mapper_args__ = {
        'primary_key': {Company, ItemCode, SupplierCode, PurUnit, ValidFrom, ValidTo}
    }
    '''

    @hybrid_property
    def ItemName(self):
        return self.LangColumn('ItemName_')


    @hybrid_method
    def ClassCode(self,index):
        return self.getColumn('Class%sCode' % index)

    @classmethod
    def list_del(cls, division, itemCodes, *supplierCodes):
        filters = [ cls.Division == division, cls.ItemCode.in_(itemCodes),
                  cls.ValidFrom <= date.today(), cls.ValidTo >= date.today()]
        if len(supplierCodes) > 0:
            filters.append(cls.SupplierCode.in_(supplierCodes))

        return cls.query.filter(*filters).all()

    @classmethod
    def controls(cls,division,costCenterCode,date, type):
        dcc = DataControlConfig.list('ControlSupplierBySite',date) \
            .drop(['Id', 'Guid', 'Type', 'StartDate', 'EndDate'], axis=1)
        dcc.rename(columns={'Val1': 'Division', 'Val2': 'CostCenterCode',
                            'Val3': 'ClassIndex', 'Val4': 'ClassCode',
                            'Val5': 'SupplierCode', 'Val6': 'Type',
                            'Val7': 'CostCenterCategory'}, inplace=True)
        DataFrameSetNan(dcc)
        tmp = CostCenter.GetServiceCategory(costCenterCode)
        dcc = dcc[((dcc['Type']=='')|(dcc['Type']==type ))&
                  ((dcc['Division']=='')|(dcc['Division']==division))&
                  ((dcc['CostCenterCode']=='')|(dcc['CostCenterCode']==costCenterCode))]

        dcc = dcc[dcc.apply(lambda x: True if x['CostCenterCategory']=='' or
                                    (tmp in ',%s,' % x['CostCenterCategory']) else False, axis=1)]
        return dcc

    @classmethod
    def list(cls,costCenterCode, date,type,needSuppliers=False,division=None):
        if not division:
            division = CostCenter.GetDivision(costCenterCode)
        tmpcontrols = cls.controls(division,costCenterCode,date,type)
        classFields = [cls.ClassCode(l['ClassIndex'])
                       for l in getdict(tmpcontrols.loc[
                                            (tmpcontrols['ClassIndex'] !='')&(tmpcontrols['ClassCode'] !=''),
                                            ['ClassIndex','ClassCode']])]

        sql = cls.query.filter(cls.Division==division,cls.ValidFrom<=date,cls.ValidTo>=date)\
            .with_entities(cls.ItemCode,cls.ItemName.label('ItemName'),
                           cls.SupplierCode.label('SupplierCode'),cls.SupplierCode.label('SupplierName'),
                           cls.Price,cls.Tax,
                           cls.PurUnit.label('PurUnit'),cls.StockUnit.label('StockUnit'),
                           cls.BOMUnit.label('BOMUnit'),
                           cls.PurStkConversion.label('PurStkConversion'),
                           cls.StkBOMConversion.label('StkBOMConversion'),*classFields)
        items = pd.read_sql(sql.statement,cls.getBind())

        if tmpcontrols.empty: return items
        DataFrameSetNan(items)

        def _control(item,itemcontrols):
            itemcontrols = getdict(itemcontrols,['ClassIndex','ClassCode','SupplierCode'])
            for l in itemcontrols:
                if (l['ClassIndex']=='' or l['ClassCode']=='' or
                        item['Class%sCode' % l['ClassIndex']]==l['ClassCode']) and \
                        (l['SupplierCode']=='' or item['SupplierCode']==l['SupplierCode']):
                    return True
            return False

        for name,group in tmpcontrols.groupby(by=['Type']):
            # 全部产品大类，全部供应商
            if not group[((group['ClassIndex']=='')|(group['ClassCode']==''))&(group['SupplierCode']=='')].empty:
                continue

            items = items[items.apply(lambda x: _control(x,group),axis=1)]

        if items.empty: Error(lang('D08CA9F5-3BA5-4DE6-9FF8-8822E5ABA1FF')) # No data

        ret= items.groupby(by=['ItemCode','ItemName','PurUnit','StockUnit','BOMUnit',
                               'PurStkConversion','StkBOMConversion'],as_index=False).agg({'Price':min})
        if needSuppliers:
            ret['suppliers'] = ret.apply(lambda x: [{k:v for k,v in l.items()
                                                     if k in ['SupplierCode','SupplierName','Price','Tax']}
                                                    for l in getdict(items[items['ItemCode']==x['ItemCode']])] ,axis=1)

        return ret