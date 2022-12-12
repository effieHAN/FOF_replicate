# -*- coding: utf-8 -*-

import pandas as pd

from FOF.util.getfundcode import getfundcodeclass_
from database.code.windsql.chinamutualfundnav import chinamutualfundnav_
from database.code.windsql.cmfindexeod import cmfindexeod_
from event.calceventretclass import calceventretclass_
from util.WindTradingDay import WindTradingDay_
from util.utilfun import rank_into_group_
from util.utilfun import tdatesshift_, getLastTdates_, plotNets_


class calceventsplitclass_(calceventretclass_):

    def __init__(self, prev_window_num=120, next_window_num=60):
        super().__init__(prev_window_num, next_window_num)
        self.fcdclass = None
        self.eret_quantle = None

    def initdataclass_(self):
        self.priceclass = chinamutualfundnav_()
        self.fcdclass = getfundcodeclass_()
        self.idxpriceclass = cmfindexeod_()

    def getpricedata_(self, idxcode='885001.WI'):
        """
        整理事件所需价格数据
        """
        # print(self.datecode)
        mindate = WindTradingDay_(fromdt=None, todt=self.datecode['date'].min())['date'].iloc[-self.prev_window_num]
        maxdate = WindTradingDay_(fromdt=self.datecode['date'].max(), todt=None)['date'].iloc[self.next_window_num]
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

    def calceventret_(self, datecode=None, plotTF=False):
        if datecode is None:
            datecode = self.datecode
        eretlist = []
        for i in range(-self.prev_window_num, 0, 1):
            datemap = tdatesshift_(self.eventtdates, i)
            datemap.columns = ['date', 'enddate']
            datecodetemp = pd.merge(datecode, datemap, on=['date'], how='inner')
            datecodetemp = datecodetemp.set_index(['date', 'code'])
            datecodetemp['price1'] = self.datecodeprice['price']
            datecodetemp = datecodetemp.reset_index().set_index(['enddate', 'code'])
            datecodetemp['price2'] = self.datecodeprice['price']
            datecodetemp = datecodetemp.reset_index()

            datecodetemp = datecodetemp.set_index(['date'])
            datecodetemp['idxprice1'] = self.dateidxprice['idxprice']
            datecodetemp = datecodetemp.reset_index().set_index(['enddate'])
            datecodetemp['idxprice2'] = self.dateidxprice['idxprice']
            datecodetemp = datecodetemp.reset_index()
            datecodetemp['stkret'] = datecodetemp['price1'] / datecodetemp['price2'] - 1
            datecodetemp['idxret'] = datecodetemp['idxprice1'] / datecodetemp['idxprice2'] - 1

        datecode1 = tdatesshift_(self.eventtdates, 20)
        datecode1.columns=['date','tdate']
        datecode1 = pd.merge(datecode, datecode1, on=['date'], how='inner')
        print(datecode1)
        datecode1 = datecode1[['tdate', 'code']]
        datecode1.columns=['date','code']
        for i in range(20,self.next_window_num + 1,1):
            datemap = tdatesshift_(sorted(set(datecode1['tdate'])), i)
            datemap.columns = ['date', 'enddate']
            datecodetemp = pd.merge(datecode1, datemap, on=['date'], how='inner')
            datecodetemp = datecodetemp.set_index(['date', 'code'])
            datecodetemp['price1'] = self.datecodeprice['price']
            datecodetemp = datecodetemp.reset_index().set_index(['enddate', 'code'])
            datecodetemp['price2'] = self.datecodeprice['price']
            datecodetemp = datecodetemp.reset_index()

            datecodetemp = datecodetemp.set_index(['date'])
            datecodetemp['idxprice1'] = self.dateidxprice['idxprice']
            datecodetemp = datecodetemp.reset_index().set_index(['enddate'])
            datecodetemp['idxprice2'] = self.dateidxprice['idxprice']
            datecodetemp = datecodetemp.reset_index()

            datecodetemp['stkret'] = datecodetemp['price2'] / datecodetemp['price1'] - 1
            datecodetemp['idxret'] = datecodetemp['idxprice2'] / datecodetemp['idxprice1'] - 1

            datecodetemp['eret'] = datecodetemp['stkret'] - datecodetemp['idxret']
            datecodetemp['dnum'] = i
            eretlist = eretlist + [datecodetemp]
            # print(i)

        self.eretdfall = pd.concat(eretlist, axis=0, ignore_index=True)
        eventeret = self.eretdfall.groupby(['dnum'])['eret'].mean().reset_index()
        eventeret = eventeret.sort_values(['dnum']).reset_index(drop=True)
        # eventeret.loc[eventeret['dnum'] < 0, 'eret'] = -eventeret.loc[eventeret['dnum'] < 0, 'eret']
        if plotTF:
            plotNets_(eventeret[['dnum', 'eret']])
        return eventeret

    # def getquantile_(self, lookbackdays, n_group):
    #     eretdfall = self.eretdfall[self.eretdfall["eret"].notna()].copy()
    #     eretdfall=eretdfall[["enddate", 'date', 'code', 'eret', 'dnum']]
    #     quse = eretdfall[eretdfall["dnum"] == -lookbackdays].copy()
    #     quse.loc[:, "rank"] = pd.qcut(quse["eret"], n_group, labels=False).values
    #     quse=quse[quse["rank"].notna()].copy()
    #     eretuse = pd.merge(eretdfall, quse[['date', 'code', "rank", ]], on=['date', 'code'], how="left")
    #     eretuse = eretuse.sort_values(['rank']).reset_index(drop=True)
    #     quantiles = []
    #     for i in eretuse["rank"].unique():
    #         temp = eretuse[eretuse["rank"] == i]
    #         temp = temp.groupby(["dnum"])["eret"].mean().reset_index()
    #         temp = temp.sort_values(['dnum']).reset_index(drop=True)
    #         temp = temp.rename(columns={"eret": ("Q" + str(i + 1))})
    #         quantiles.append(temp)
    #
    #     quantiles = [df.set_index("dnum") for df in quantiles]
    #     quantiles = pd.concat(quantiles, axis=1)
    #     return quantiles

# if __name__ == '__main__':
#     self = calceventfundclass_(prev_window_num=120, next_window_num=60)
#     self.initdataclass_()
#     import pandas as pd
#     self.datecode = pd.read_csv('D:/datecode.csv')
#     self.datecode.columns = ['date', 'fundcode']
#     self.datecode['date'] = self.datecode['date'].astype(str)
#     self.datecode = self.datecode[self.datecode['date'] <= '20220708']
#     self.getpricedata_()
# -*- coding: utf-8 -*-
