# -*- coding: utf-8 -*-
"""
Created on Mon Jul 15 13:08:11 2019

# the successor performance was evaluated by the latest fund performance before the current one
"""

import pandas as pd
from FOF.calcic.calmngericclass import calmngeric_
from FOF.code.mngerleave.util.util_mnger_leave import fund_detail_, mnger_detail_
from FOF.event.calceventfundclass import calceventfundclass_
from FOF.util.allstkfundlist import allstkfundlist_
from FOF.util.getfundcode import getfundcodeclass_
from FOF.util.rawsigs.size import size_
from database.code.windsql.chinamutualfundmanager import chinamutualfundmanager_
from database.code.windsql.chinamutualfundnav import chinamutualfundnav_
from util.WindTradingDay import WindTradingDay_
from util.utilfun import get_first_tradingday_month_

# In[]
startdate = '20100104'
enddate = '20220708'
# enddate = '20200101'


lookbackdays = 60
lookfutdays = 60
past_interval = 240
mngerclass = chinamutualfundmanager_()
navclass = chinamutualfundnav_()
fcdclass = getfundcodeclass_()
fclass = size_()
eventclass = calceventfundclass_(prev_window_num=lookbackdays, next_window_num=lookfutdays)
eventclass.initdataclass_()
icclass = calmngeric_(prev_window_num=past_interval)
fundlistclass = allstkfundlist_('allstkfund')

# In[]
mngerdata = mngerclass.getdata_(maxdate=enddate)
# raise Exception()

mngerdata = fcdclass.getfac_(orgdata=mngerdata, date=enddate)
mngerdatause = mngerdata[mngerdata['leavedate'] != 'nan'].copy()
mngerdatause = get_first_tradingday_month_(mngerdatause, datenameuse="leavedate")

mfdates = sorted(set(mngerdatause['date']))
mfdates = [x for x in mfdates if (x >= startdate) & (x <= enddate)]
# mfdates = [x for x in mfdates if ]

fundlist = fundlistclass.getfac_with_fundcode_(mfdates)
fundlist = fundlist[fundlist['fundtypecode'].isin(['2001010101', '2001010201', '2001010204']) &
                    (fundlist['pos0'] > 0.7) &
                    (fundlist['pos1'] > 0.7) &
                    (fundlist['pos2'] > 0.7) &
                    (fundlist['pos3'] > 0.7) &
                    (fundlist['listdays'] > (lookbackdays + 180))].reset_index(drop=True).copy()

# get vaild fund for event study
fundlist["mfdate"] = fundlist["date"]
mngerdatause = pd.merge(mngerdatause, fundlist, on=["date", "fundcode"], how="inner")
fclass.initdataclass_()
sigdata = fclass.getfac_with_fundcode_(periods=mfdates)
sigdata.columns = ['date', 'fundcode', 'sig']
mngerdatause = pd.merge(mngerdatause, sigdata, on=["date", "fundcode"], how="inner")

# keep only funds with multi mngers
nomnger = mngerdatause.groupby(["fundcode"])["fundmanagerid"].count()
turnover = pd.merge(mngerdatause, nomnger, on=['fundcode'], how="inner")
turnover = turnover.rename(columns={"fundmanagerid_x": "fundmanagerid", "fundmanagerid_y": "no_mnger"})
turnoveruse = turnover[turnover["no_mnger"] > 1].copy()

# decide whether next mnger has previous career or not

groupbyfund = turnoveruse.groupby(["fundcode"]).apply(lambda x: fund_detail_(x))
groupbyfund = groupbyfund.reset_index(drop=True)
groupbymnger = groupbyfund.groupby(["fundmanagerid"]).apply(lambda x: mnger_detail_(x))
groupbymnger = groupbymnger.reset_index(drop=True)
no_pervious = groupbymnger[groupbymnger["last_fundcode"].isnull()]
has_previous = groupbymnger[groupbymnger["last_fundcode"].notna()]
# for group has previous cal ancestor ic and successor ic
datecode_pre_ancestor = has_previous[["last_leavedate", "fundcode"]].copy()
datecode_pre_successor = has_previous[["last_leavedate", "last_fundcode"]].copy()
##ic for ancestor
icclass.initdataclass_()
datecode_pre_ancestor = icclass.geteventdf_(datecode=datecode_pre_ancestor, startdate=startdate, enddate=enddate)
icclass.getpricedata_(idxcode='885001.WI')
inforatio_ancestor = icclass.cal_ic_()
inforatio_ancestor = pd.merge(inforatio_ancestor, datecode_pre_ancestor, on=["code"], how="inner")
inforatio_ancestor = inforatio_ancestor.rename(columns={"date": "last_leavedate", "code": "fundcode", "IC": "IC_an"})
##ic for successor
datecode_pre_successor = icclass.geteventdf_(datecode=datecode_pre_successor, startdate=startdate, enddate=enddate)
inforatio_successor = icclass.cal_ic_()
inforatio_successor = pd.merge(inforatio_successor, datecode_pre_successor, on=["code"], how="inner")
inforatio_successor = inforatio_successor.rename(columns={"date": "last_leavedate", "code": "last_fundcode", "IC": "IC_su"})
# raise Exception()

##ic compare
ic_compare = pd.merge(has_previous[["fundcode", "fundmanagerid", "last_leavedate", "last_fundcode", "startdate"]],
                      inforatio_ancestor, on=["last_leavedate", "fundcode"], how="inner")
ic_compare = pd.merge(ic_compare, inforatio_successor, on=["last_leavedate", "last_fundcode"], how="inner")

better_su = ic_compare[ic_compare["IC_an"] < ic_compare["IC_su"]]
worse_su = ic_compare[ic_compare["IC_an"] > ic_compare["IC_su"]]

# cal average eret
##better successor
datecode_better = better_su[['startdate', "fundcode"]].copy()
datecode_better = datecode_better[(datecode_better['startdate'] <= WindTradingDay_(fromdt=None, todt=enddate)['date'].iloc[-lookfutdays - 1])]
eventclass.initdataclass_()
datecode_better = eventclass.geteventdf_(datecode=datecode_better, startdate=startdate, enddate=enddate)
eventclass.getpricedata_(idxcode='885001.WI')
eventeret_better_su = eventclass.calceventret_(plotTF=True)

##worse successor
datecode_worse = worse_su[['startdate', "fundcode"]].copy()
datecode_worse = datecode_worse[(datecode_worse['startdate'] <= WindTradingDay_(fromdt=None, todt=enddate)['date'].iloc[-lookfutdays - 1])]
datecode_worse = eventclass.geteventdf_(datecode=datecode_worse, startdate=startdate, enddate=enddate)
eventeret_worse_su = eventclass.calceventret_(plotTF=True)

##no previous group
datecode_no_previous = no_pervious[["startdate", "fundcode"]].copy()
datecode_no_previous = datecode_no_previous[
    (datecode_no_previous['startdate'] <= WindTradingDay_(fromdt=None, todt=enddate)['date'].iloc[-lookfutdays - 1])]
datecode_no_previous = eventclass.geteventdf_(datecode=datecode_no_previous, startdate=startdate, enddate=enddate)
eventeret_no_previous = eventclass.calceventret_(plotTF=True)
