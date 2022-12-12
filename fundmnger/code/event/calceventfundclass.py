# -*- coding: utf-8 -*-

import pandas as pd

from FOF.util.getfundcode import getfundcodeclass_
from database.code.windsql.chinamutualfundnav import chinamutualfundnav_
from database.code.windsql.cmfindexeod import cmfindexeod_
from event.calceventretclass import calceventretclass_
from util.WindTradingDay import WindTradingDay_
from util.utilfun import rank_into_group_


class calceventfundclass_(calceventretclass_):

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

    def calqeret_(self, lookbackdays, n_group):
        eretdfall = self.eretdfall[self.eretdfall["eret"].notna()].copy()
        quse = eretdfall[eretdfall["dnum"] == -lookbackdays].copy()
        quse["rank"] = rank_into_group_(quse["eret"], groupnum=n_group)
        eretuse = pd.merge(eretdfall, quse[['date', 'code', "rank"]], on=['date', 'code'], how="inner")
        self.eret_quantle = eretuse
        # qeret = eretuse.groupby(['rank', 'dnum'])['eret'].mean()
        eretuse = eretuse.sort_values(['rank']).reset_index(drop=True)
        quantiles = []
        for i in eretuse["rank"].unique():
            temp = eretuse[eretuse["rank"] == i]
            temp = temp.groupby(["dnum"])["eret"].mean().reset_index()
            temp = temp.sort_values(['dnum']).reset_index(drop=True)
            temp = temp.rename(columns={"eret": ("Q" + str(i))})
            quantiles.append(temp)

        quantiles = [df.set_index("dnum") for df in quantiles]
        quantiles = pd.concat(quantiles, axis=1)
        return quantiles

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
