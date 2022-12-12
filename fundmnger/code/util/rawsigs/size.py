# -*- coding: utf-8 -*-
"""
Created on Sun Mar  8 09:20:48 2020

@author: liukai
"""

from FOF.util.factorclass import fundfactor
from database.code.windsql.chinamutualfundassetportfolio import chinamutualfundassetportfolio_
from util.WindTradingDay import WindTradingDay_


# In[] 因子测试
class size_(fundfactor):

    def __init__(self, facname='size', facnameuse='size'):
        """
        数据默认就是总体规模，不分A类B类等类别
        """
        super().__init__(facname, facnameuse)
        # 可能会用到的基础数据
        self.asset = None

    def initdataclass_(self):
        self.asset = chinamutualfundassetportfolio_()

    def getfapuse_(self, date):
        LTD = WindTradingDay_(fromdt=None, todt=date)['date'].iloc[-2]
        startdate = WindTradingDay_(fromdt=None, todt=date)['date'].iloc[-100]
        fapuse = self.asset.getdata_(varnames=['netasset'], fromdt=startdate, todt=LTD)
        fapuse = fapuse.sort_values(['fundcode', 'rptdate'], ascending=[True, False]).reset_index(drop=True)
        fapuse = fapuse[~fapuse.duplicated(['fundcode'])].reset_index(drop=True)
        return fapuse

    def sigconstruct_(self, date=None):
        fapuse = self.getfapuse_(date)
        ffuse = fapuse[['fundcode', 'netasset']]
        ffuse.columns = ['fundcode', 'sig']

        factorfinal = self.solvefac_(ffuse[['fundcode', 'sig']], date)
        return factorfinal


# In[]
# if __name__ == '__main__':
#     periods = WindTradingDay_(fromdt='20100101', todt='20220104', IfMonthFirst=True)['date'].tolist()
#     fclass = size_()
#     # fclass.initdataclass_()
#     sigdata = fclass.getfac_with_fundcode_(periods=periods)
#     sigdata.columns = ['date', 'fundcode', 'sig']
#     sigdata[['date', 'fundcode', 'sig']].to_csv('D:/proj/CCBFund/sigtest/sigdata.csv', sep=',', index=False)
