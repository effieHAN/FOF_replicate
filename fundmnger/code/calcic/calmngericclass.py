# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np

from FOF.util.getfundcode import getfundcodeclass_
from database.code.windsql.chinamutualfundnav import chinamutualfundnav_
from database.code.windsql.cmfindexeod import cmfindexeod_
from util.WindTradingDay import WindTradingDay_
from util.utilfun import tdatesshift_, getLastTdates_
from FOF.code.mngerleave.util.util_mnger_leave import compute_ic_


class calmngeric_():

    def __init__(self, prev_window_num=120):
        self.fcdclass = None
        self.prev_window_num = prev_window_num
        self.priceclass = None
        self.idxpriceclass = None
        self.datecode = None
        self.datecodeprice = None
        self.dateidxprice = None
        self.eventtdates = None
        self.eretdfall = None

    def initdataclass_(self):
        self.priceclass = chinamutualfundnav_()
        self.fcdclass = getfundcodeclass_()
        self.idxpriceclass = cmfindexeod_()

    def geteventdf_(self, datecode=None, startdate=None, enddate=None):
        """
        日期可以是自然日，在这个方法中将日期转换为交易日
        """
        datecode = datecode.copy()
        datecode.columns = ['ndate', 'code']
        datemap = getLastTdates_(sorted(set(datecode['ndate'])))
        datemap.columns = ['ndate', 'date']
        datecode = pd.merge(datecode, datemap, on=['ndate'], how='inner')
        datecode = datecode[['date', 'code']].copy()
        if startdate is None and enddate is not None:
            datecode = datecode[(datecode['date'] <= enddate)].reset_index(drop=True)
        elif startdate is not None and enddate is None:
            datecode = datecode[(datecode['date'] >= startdate)].reset_index(drop=True)
        elif startdate is not None and enddate is not None:
            datecode = datecode[(datecode['date'] >= startdate) & (datecode['date'] <= enddate)].reset_index(drop=True)
        else:
            datecode = datecode
        self.eventtdates = sorted(set(datecode['date']))
        self.datecode = datecode
        return datecode

    def getpricedata_(self, idxcode='885001.WI'):
        """
        整理事件所需价格数据
        """
        # print(self.datecode)
        mindate = WindTradingDay_(fromdt=None, todt=self.datecode['date'].min())['date'].iloc[-self.prev_window_num]
        maxdate = WindTradingDay_(fromdt=self.datecode['date'].max(), todt=None)['date'].iloc[0]
        datecodeprice = self.priceclass.getdata_(varnames=['date', 'navadj'], fromdt=mindate, todt=maxdate)
        datecodeprice = self.fcdclass.getfac_(orgdata=datecodeprice[['date', 'fundcode', 'navadj']],
                                              date=self.datecode['date'].max())
        datecodeprice.columns = ['date', 'code', 'price']
        self.datecodeprice = datecodeprice.set_index(['date', 'code'])

        dateidxcodeprice = self.idxpriceclass.getdata_(idxcodelist=[idxcode], varnames=['close'],
                                                       fromdt=mindate, todt=maxdate)
        dateidxprice = dateidxcodeprice[['date', 'close']].copy()
        dateidxprice.columns = ['date', 'idxprice']
        self.dateidxprice = dateidxprice.set_index(['date'])

    def cal_ic_(self, datecode=None):
        if datecode is None:
            datecode = self.datecode
        eretlist = []
        for i in range(-self.prev_window_num, 1, 1):
            # print(i)
            datemap = tdatesshift_(self.eventtdates, i)
            datemap.columns = ['date', 'enddate']
            # print(datemap)
            datecodetemp = pd.merge(datecode, datemap, on=['date'], how='inner')
            datecodetemp = datecodetemp.reset_index().set_index(['enddate', 'code'])
            datecodetemp['price'] = self.datecodeprice['price']
            datecodetemp = datecodetemp.reset_index()

            datecodetemp = datecodetemp.reset_index().set_index(['enddate'])
            datecodetemp['idxprice'] = self.dateidxprice['idxprice']
            datecodetemp = datecodetemp.reset_index()
            datecodetemp['dnum'] = i
            eretlist = eretlist + [datecodetemp]
            # print(i)
        eretdfall = pd.concat(eretlist, axis=0, ignore_index=True)
        eretdfall = eretdfall.sort_values("dnum")
        eretdfall['fundret'] = eretdfall.groupby("code")['price'].apply(lambda x: np.log(x) - np.log(x.shift(1)))
        eretdfall['idxret'] = eretdfall.groupby("code")['idxprice'].apply(lambda x: np.log(x) - np.log(x.shift(1)))
        eretdfall['eret'] = eretdfall['fundret'] - eretdfall['idxret']
        # print(eretdfall)
        self.eretdfall = eretdfall
        info_ratio = eretdfall.groupby("code")["eret"].apply(lambda x: compute_ic_(x))
        info_ratio = pd.DataFrame(info_ratio)
        info_ratio = info_ratio.rename(columns={"eret": "IC"})
        info_ratio = info_ratio.sort_values(['code']).reset_index()
        return info_ratio

# #
if __name__ == '__main__':
    self = calmngeric_(prev_window_num=60)
    self.initdataclass_()
    import pandas as pd
    self.datecode = pd.read_csv('D:/datecode.csv')
    self.datecode.columns = ["index",'date', 'fundcode']
    datecode=self.datecode[['date', 'fundcode']].copy()
    datecode['date'] = datecode['date'].astype(str)
    # datecode=sorted(set(datecode["date"]))
    self.geteventdf_(datecode=datecode)
    self.getpricedata_()
    info=self.cal_ic_()
    eretdfall=self.eretdfall
