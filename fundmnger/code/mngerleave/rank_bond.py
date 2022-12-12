# -*- coding: utf-8 -*-
import pandas as pd

import matplotlib.pyplot as plt
import pandas as pd

from FOF.code.mngerleave.util.util_mnger_leave import cal_dsig_, cal_overlap_
from FOF.code.mngerleave.util.util_mnger_leave import keep_change_
from FOF.event.calceventfundclass import calceventfundclass_
from FOF.util.allstkfundlist import allstkfundlist_
from FOF.util.funddataoutputclass import funddataoutput_
from FOF.util.getfundcode import getfundcodeclass_
from FOF.util.rawsigs.size import size_
from database.code.windsql.chinamutualfundmanager import chinamutualfundmanager_
from database.code.windsql.chinamutualfundnav import chinamutualfundnav_
from util.WindTradingDay import WindTradingDay_
from util.utilfun import get_first_tradingday_month_

# In[]
startdate = '20100104'
enddate = '20220708'
lookbackdays = 120
lookfutdays = 120
mngerclass = chinamutualfundmanager_()
navclass = chinamutualfundnav_()
fcdclass = getfundcodeclass_()
eventclass = calceventfundclass_(prev_window_num=lookbackdays, next_window_num=lookfutdays)
fundlistclass = allstkfundlist_('allstkfund')
fclass = size_()
# funddataoutputclass = funddataoutput_()
#
# mngerdata = mngerclass.getdata_(maxdate=enddate)
# mngerdata = fcdclass.getfac_(orgdata=mngerdata, date=enddate)
# mngerdatause = mngerdata[mngerdata['leavedate'] != 'nan'].copy()
# mngerdatause = get_first_tradingday_month_(mngerdatause, datenameuse="leavedate")
#
# mfdates = sorted(set(mngerdatause['date']))
# # mfdates = [x for x in mfdates if x >= '20100104']
# mfdates = [x for x in mfdates if (x >= startdate) & (x <= enddate)]
date=pd.date_range('2022-08-01','2022-11-11')
date=[pd.Timestamp(x).strftime('%Y-%m-%d') for x in date.values]
date=[x.replace("-",'') for x in date]
# date=['2022']
fundlist = fundlistclass.getfac_with_fundcode_(date)
data=pd.read_excel(r"C:\Users\effie\Desktop\bond.xlsx")
codes=data['证券代码'].to_list()
fundlist = fundlist[fundlist['fundtypecode'].isin(codes &
                    (fundlist['listdays'] >  120))].reset_index(drop=True).copy()

raise Exception()
datecode = mngerdatause[['date', "fundcode"]].copy()
datecodeuse = datecode[(datecode['date'] <= WindTradingDay_(fromdt=None, todt=enddate)['date'].iloc[-lookfutdays - 1])]
# cal excess return for leave funds (overall and percentile )
eventclass.initdataclass_()
datecode = eventclass.geteventdf_(datecode=datecodeuse, startdate=startdate, enddate=enddate)
eventclass.getpricedata_(idxcode='885001.WI')
eventeret = eventclass.calceventret_(plotTF=True)
eret_ind_leave = eventclass.eretdfall