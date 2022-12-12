# -*- coding: utf-8 -*-
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 15 13:08:11 2019


"""

import matplotlib.pyplot as plt
import pandas as pd

from FOF.code.mngerleave.util.util_mnger_leave import cal_dsig_, cal_overlap_
from FOF.code.mngerleave.util.util_mnger_leave import keep_change_for_startdate_
from FOF.event.calceventsplit0 import calceventsplitclass_
from FOF.util.allstkfundlist import allstkfundlist_
from FOF.util.funddataoutputclass import funddataoutput_
from FOF.util.getfundcode import getfundcodeclass_
from FOF.util.rawsigs.size import size_
from database.code.windsql.chinamutualfundmanager import chinamutualfundmanager_
from database.code.windsql.chinamutualfundnav import chinamutualfundnav_
from util.WindTradingDay import WindTradingDay_
from util.utilfun import get_first_tradingday_month_
from util.utilfun import tdatesshift_,plotNets_

# In[]
startdate = '20100104'
enddate = '20220708'
lookbackdays = 120
lookfutdays = 120
mngerclass = chinamutualfundmanager_()
navclass = chinamutualfundnav_()
fcdclass = getfundcodeclass_()
eventclass = calceventsplitclass_(prev_window_num=lookbackdays, next_window_num=lookfutdays)
fundlistclass = allstkfundlist_('allstkfund')
fclass = size_()
funddataoutputclass = funddataoutput_()

mngerdata = mngerclass.getdata_(maxdate=enddate)
mngerdata = fcdclass.getfac_(orgdata=mngerdata, date=enddate)
# mngerdatause = mngerdata[mngerdata['leavedate'] != 'nan'].copy()
# raise Exception()
mngerdatause = get_first_tradingday_month_(mngerdata, datenameuse="startdate")

mfdates = sorted(set(mngerdatause['date']))
mfdates = [x for x in mfdates if (x >= startdate) & (x <= enddate)]

# raise Exception()
fundlist = fundlistclass.getfac_with_fundcode_(mfdates)
fundlist = fundlist[fundlist['fundtypecode'].isin(['2001010101', '2001010201', '2001010204']) &
                    (fundlist['pos0'] > 0.7) &
                    (fundlist['pos1'] > 0.7) &
                    (fundlist['pos2'] > 0.7) &
                    (fundlist['pos3'] > 0.7) &
                    (fundlist['listdays'] > (lookbackdays + 180))].reset_index(drop=True).copy()
# get vaild fund for event study
mngerdatause = pd.merge(mngerdatause, fundlist, on=["date", "fundcode"], how="inner")
#
# only keep those that changes the fund company
fundcodelist = sorted(set(mngerdatause['fundcode']))
compname = funddataoutputclass.getcominfo_(maxdate=enddate, fundcodelist=fundcodelist)
mngerdatause = pd.merge(mngerdatause, compname, on=['fundcode'], how='inner')
# raise Exception()
# subdata=mngerdatause[mngerdatause['fundmanagerid']=='00929']
# subdata_done=keep_change_for_startdate_(subdata)
mngerdatause = mngerdatause.groupby(['fundmanagerid']).apply(lambda x: keep_change_for_startdate_(x))
mngerdatause = mngerdatause.reset_index(drop=True)
# data_check=mngerdatause.set_index(['fundmanagerid'])[['fundcode',"compname"]]

datecode = mngerdatause[['date', "fundcode"]].copy()
datecodeuse = datecode[(datecode['date'] <= WindTradingDay_(fromdt=None, todt=enddate)['date'].iloc[-lookfutdays - 1])]
# cal excess return for leave funds (overall and percentile )
eventclass.initdataclass_()
datecode = eventclass.geteventdf_(datecode=datecodeuse, startdate=startdate, enddate=enddate)
eventclass.getpricedata_(idxcode='885001.WI')
eventeret = eventclass.calceventret_(plotTF=True)
# eret_ind_leave = eventclass.eretdfall
# plt.show()
