# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from pandas import merge
from sqlalchemy import func, and_, or_, literal_column, case, cast
from sqlalchemy.ext.hybrid import hybrid_property

from .OrderLine import OrderLine
from ..Stock import TransData
from ..common.Calendar import Calendar
from ..common.CostCenter import CostCenter
from ..common.DataControlConfig import DataControlConfig
from ..common.LangMast import lang
from ....utils.QRCode import QRCodeBytes
from ....utils.functions import *
from ...erp import erp, db
from .OrderLine import OrderLine
from ....entity import SaveDB


class OrderHead(erp):
    __tablename__ = 'tblOrderHead'

    HeadGuid = db.Column()
    OrderNo = db.Column()
    OrderDate = db.Column(db.Date)
    OrderType = db.Column()
    OrderSubType = db.Column()
    CostCenterCode = db.Column()
    CreateUser = db.Column()
    CreateTime = db.Column(db.DateTime, default=datetime.datetime.now,onupdate=datetime.datetime.now)
    AppGuid = db.Column()
    AppStatus = db.Column()
    FromType = db.Column()
    Active = db.Column(db.Boolean, default=True)

    lines = db.relationship('OrderLine', primaryjoin='OrderHead.HeadGuid == foreign(OrderLine.HeadGuid)',
                            lazy='joined')

    @classmethod
    def getConfigs(cls, costCenterCode, type, subtype, field):
        df =  DataControlConfig.list(['PO', 'FoodPO'])
        if df.empty: return ''
        df = df[(df['Type'] == type) & (df['Val1'] == subtype)]
        if df.empty: return ''

        DataFrameSetNan(df)
        tmpdf = df[df['Val2'] == CostCenter.GetDivision(costCenterCode)]
        if not tmpdf.empty:
            df = tmpdf

        return df.iloc[0][field]

    @classmethod
    def getFoodPODeadLine(cls, OrderType, OrderSubType, costCenterCode, date):
        if OrderType.lower() =='nofoood' or OrderSubType.lower() != 'normal':
            return None

        if isinstance(date,datetime.date):
            date = getDateTime(date)
        deadline = getDateTime(date + ' ' + cls.getConfigs(costCenterCode,'FoodPO','DailyEndTime','Val3'))

        createTime = datetime.datetime.now() + datetime.timedelta(days=getInt(cls.getNextDays(costCenterCode)),minutes=1)
        if deadline.date() > createTime.date():
            return None

        if deadline < createTime:
            return -1

        return (deadline-createTime).seconds

    @classmethod
    def getNextDays(cls,costCenterCode):
        return getInt(cls.getConfigs(costCenterCode,'FoodPO','NextDays','Val3'))

    @classmethod
    def getPlanningPOWeeks(cls, costCenterCode):
        return getInt(cls.getConfigs(costCenterCode, 'FoodPO', 'PlanningPOWeeks', 'Val3'))

    # 可以做周单的星期几
    @classmethod
    def getStartWeekDay(cls, costCenterCode):
        return getWeekDay(cls.getConfigs(costCenterCode,'FoodPO','WeekDayToWeekPOs', 'Val3'))

    # 可以做几天前的补单和非s食品PO
    @classmethod
    def getEarliestDays(cls, costCenterCode):
        return  getInt(cls.getConfigs(costCenterCode, 'PO', 'EarliestDays', 'Val3'))

    # NoFood:
    # Food: 补单，普通单
    @classmethod
    def dates(cls, costCenterCode, orderType):
        # 普通单的日期
        today = datetime.date.today()
        date = today
        if orderType.lower() == 'food':
            date += datetime.timedelta(days=cls.getNextDays(costCenterCode))

        workdates = Calendar.WorkDates()
        earLiestDate = sorted([d for d in workdates if d < date],
                              reverse=True)[cls.getEarliestDays(costCenterCode)]
        # earLiestDate = datetime.date.today() - datetime.timedelta(days=cls.getEarliestDays(costCenterCode))
        ret = {'EarliestDate': getDateTime(earLiestDate)}

        if orderType.lower() != 'food':
            filters = [cls.CostCenterCode==costCenterCode,
                       cls.Active==True, cls.OrderType==orderType,
                       cls.OrderDate>=earLiestDate,
                       ~cls.lines.any(func.abs(func.round(OrderLine.Qty+
                                                          func.coalesce(OrderLine.AdjQty,0)-
                                                          OrderLine.RemainQty,6))>0)]
            tmp = cls.query.filter(*filters).with_entities(cls.OrderDate).distinct().order_by(cls.OrderDate).all()
            ret['dates'] = [{'date': getDateTime(d.OrderDate)} for d in tmp]
            return ret

        deadline = cls.getFoodPODeadLine(orderType,'Normal',costCenterCode,date)
        if not deadline and deadline < 0: date = date + datetime.timedelta(days=1)

        dates2 = []
        #补单
        d = earLiestDate
        while(d<date):
            dates2.append({'date':getDateTime(d),'remark':'Addition Order'})
            d = d + datetime.timedelta(days=1)

        dates1 = []
        remark = 'This Week'
        if not isSameWeek(today,date): #date.weekday() < today.weekday() or (date - today).days>6:
            remark = 'Next Week'

        # 周四、五上班，周六、日休息，下周一开始周单
        canDoNextWeek = False
        tmp = sorted([d for d in workdates if d >= date])
        # tmp[1] or tmp[0]是下一周
        if not isSameWeek(today,tmp[1]):
            canDoNextWeek = True

        startWeekDay = cls.getStartWeekDay(costCenterCode)
        ppoweeks = cls.getPlanningPOWeeks(costCenterCode)
        while(True):
            dates1.append({'date':getDateTime(date),'remark':remark})

            # 周日
            if date.weekday() == 6:
                # 无下周周单, 下周的周数已到
                #if datetime.date.today().weekday() < startWeekDay or ppoweeks==0:
                if not canDoNextWeek or ppoweeks == 0:
                    break

                remark = 'Next Week'
                ppoweeks = ppoweeks - 1

            date = date + datetime.timedelta(days=1)

        ret['dates'] = dates1 + dates2
        return ret

    @classmethod
    def updatepo(cls, costCenterCode, headGuid, orderDate, orderType, orderSubType):
        if not orderDate:
            Error(lang('6AD2E6B1-CD3C-465F-9651-9929429D72A3'))

        filters = [cls.Active==True, cls.CostCenterCode == costCenterCode,
                   cls.OrderDate == orderDate, cls.OrderType == orderType,
                   ~cls.lines.any(func.abs(func.round(func.coalesce(OrderLine.Qty, 0)
                                                      + func.coalesce(OrderLine.AdjQty,0)
                                                      - func.coalesce(OrderLine.RemainQty, 0), 6)) > 0)]
        if orderSubType:
            filters.append(cls.OrderSubType==orderSubType)
        if headGuid:
            filters.appends(cls.HeadGuid==headGuid)

        tmp = cls.query.filter(*filters).all()

        # 新增
        if not headGuid and len(tmp) == 0:
            ret = {'ID':0, 'OrderNo':DataControlConfig.getPONumber()}
            deadline = cls.getFoodPODeadLine(orderType,orderSubType,costCenterCode, orderDate)
            if deadline: ret['DeadLine'] = deadline

            return ret

        if len(tmp) == 0:
            Error('Not find data')
        if len(tmp) > 1:
            Error('find many po')

        ret = {'ID': tmp[0].Id, 'HeadGuid': tmp[0].HeadGuid, 'OrderNo': tmp[0].OrderNo,
               'AppStatus': tmp[0].AppStatus}

        if tmp[0].lines:
            tt = [{k[0].lower()+k[1:]:getVal(v) for k,v in l.to_dict().items()
                   if k.lower() not in ['headguid','deletetime','createtime','guid','remainqty','qrcode']}
                  for l in tmp[0].lines]
            df = pd.DataFrame(tt)
            DataFrameSetNan(df)

            stock = TransData.ItemStock(costCenterCode,getDateTime(orderDate))

            df = merge(df,stock, how='left',left_on='itemCode',right_on='ItemCode')
            df.drop(['ItemCode'],axis=1,inplace=True)
            df['guid'] = df.apply(lambda x: getGUID(),axis=1)
            DataFrameSetNan(df)
            ret = {**ret,'PO':df.to_dict('records')}

        deadline = cls.getFoodPODeadLine(orderType,orderSubType,costCenterCode, orderDate)
        if deadline:
            ret['DeadLine'] = deadline
        return ret

    def save(self,data):
        with RunApp():
            # Food order存盘状态是submitted
            if self.AppStatus.lower() == 'new' and self.OrderType.lower() == 'food':
                self.AppStatus = 'submitted'

            if data.get('orderLines'):
                dflines = pd.DataFrame(data.get('orderLines'))

                # 新增
                if getNumber(self.Id) < 1:
                    self.HeadGuid = getGUID()
                    self.Id = None
                    if 'ID' in dflines.columns:
                        dflines.drop(['ID'], axis=1, inplace=True)

                if 'qty' not in dflines.columns:
                    dflines['qty'] = 0
                if 'adjQty' not in dflines.columns:
                    dflines['adjQty'] = 0

                DataFrameSetNan(dflines)
                dflines['remainQty'] = dflines.apply(lambda x: getNumber(x['adjQty'])+getNumber(x['qty']),  axis=1)
                # 0订单
                dflines = dflines[(dflines['remainQty'] !=0)|(dflines['qty'] !=0)]
                self.lines = [OrderLine({**getdict(l),
                                        'QRCode':QRCodeBytes('%s,%s' %(self.HeadGuid,l.get('supplierCode')))},
                                        True) for l in dflines.to_dict(orient='records')]
                # self.lines = [OrderLine(getdict(l),True) for l in dflines.to_dict(orient='records')]

            with SaveDB() as session:
                # OrderNo与HeadGuid是一对一
                if OrderHead.query.filter(OrderHead.OrderNo==self.OrderNo,
                                          OrderHead.HeadGuid != self.HeadGuid,
                                          OrderHead.Active==True).first():
                    Error(lang('BA9AB871-4BDD-4C72-A5EF-9CCA75953F36') % self.OrderNo)

                # 已入库，不能修改（不分供应商）
                if OrderLine.query.filter(OrderLine.HeadGuid==self.HeadGuid,
                                          func.abs(func.round(OrderLine.Qty
                                                              + func.coalesce(OrderLine.AdjQty, 0)
                                                              - OrderLine.RemainQty, 6)) > 0).first():
                    Error(lang('A88FDE80-74BF-4553-AB45-28F4751D74DB'))

                # Food-Normal, Food-Addition, NoFood, 未入库只能有一个HeadGuid
                if OrderHead.query.filter(OrderHead.HeadGuid != self.HeadGuid,OrderHead.Active==True,
                                          OrderHead.OrderDate==self.OrderDate,
                                          OrderHead.CostCenterCode==self.CostCenterCode,
                                          OrderHead.OrderType==self.OrderType,
                                          func.coalesce(OrderHead.OrderSubType,'')==func.coalesce(self.OrderSubType,''),
                                          ~OrderHead.lines.any(
                                              func.abs(func.round(OrderLine.Qty
                                                                 +func.coalesce(OrderLine.AdjQty,0)
                                                                 -OrderLine.RemainQty,6))>0))\
                    .with_entities(OrderHead.OrderDate,OrderHead.CreateTime).first():
                    Error(lang('FDAFA76D-11C5-4EB4-BFEF-84150D5D40F2') % self.OrderDate)

                # 非食品单只能提交一次
                if self.OrderType.lower() != 'food' and \
                        OrderHead.query.filter(OrderHead.HeadGuid == self.HeadGuid,
                                               OrderHead.AppStatus == 'submitted') \
                                .with_entities(OrderHead.HeadGuid).first():
                    return lang('74B9FC9B-3613-4570-90FA-FD9EEE8719DD'
                                if self.AppStatus.lower() == 'new'
                                else '83FB0C33-D08D-4428-9245-697617E62CA4')

                session.merge(self)

            return lang('A16AAA03-DCE8-4936-9D9E-FE23F9AE7378'
                        if self.AppStatus.lower() == 'new'
                        else '9CE7707A-A7B6-49C7-A8ED-A9A505B286A1')

    @classmethod
    def list(cls,headGuid, costCenterCode, date, supplierCode, orderType, appStatus):
        filters = [cls.HeadGuid==headGuid, cls.CostCenterCode == costCenterCode,
                   cls.AppStatus==appStatus,cls.OrderDate == date, cls.Active==True,
                    OrderLine.SupplierCode == supplierCode, OrderLine.RemainQty != 0,
                    func.lower(OrderLine.Status) != 'completed', OrderLine.DeleteTime == None]
        if orderType:
            filters.append(cls.OrderType == orderType)

        sql = cls.query.join(OrderLine, cls.HeadGuid == OrderLine.HeadGuid) \
            .filter(*filters)\
            .with_entities(OrderLine.Guid.label('orderLineGuid'),OrderLine.ItemCode.label('itemCode'),
                           OrderLine.PurchasePrice.label('purPrice'),
                           OrderLine.CreateTime,
                           OrderLine.PurStk_Conversion.label('purStk_Conversion'),
                           OrderLine.RemainQty.label('qty'), OrderLine.Remark.label('remark'))
        tmpdf = pd.read_sql(sql.statement, cls.getBind())
        if tmpdf.empty:
            return tmpdf

        tmpdf['CreateTime'] = tmpdf['CreateTime'].max().strftime('%Y-%m-%d %H:%M:%S.%f')
        # tmpdf.rename(columns={'CreateTime': 'orderLineCreateTime'}, inplace=True)

        # tmp = [{k:getVal(getattr(l,k)) for k in l.keys() if getattr(l,k)} for l in qry]
        return tmpdf    #.to_dict('records')