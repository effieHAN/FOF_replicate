# -*- coding: utf-8 -*-
"""
Created on Mon Jul 15 13:08:11 2019


"""

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
funddataoutputclass = funddataoutput_()

mngerdata = mngerclass.getdata_(maxdate=enddate)
mngerdata = fcdclass.getfac_(orgdata=mngerdata, date=enddate)
mngerdatause = mngerdata[mngerdata['leavedate'] != 'nan'].copy()
mngerdatause = get_first_tradingday_month_(mngerdatause, datenameuse="leavedate")

mfdates = sorted(set(mngerdatause['date']))
# mfdates = [x for x in mfdates if x >= '20100104']
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
# subdata=mngerdatause[mngerdatause['fundmanagerid']=='00010']
# subdata_done=keep_change_(subdata)
mngerdatause = mngerdatause.groupby(['fundmanagerid']).apply(lambda x: keep_change_(x))
mngerdatause = mngerdatause.reset_index(drop=True)
data_check=mngerdatause.set_index(['fundmanagerid'])[['fundcode',"compname"]]

datecode = mngerdatause[['date', "fundcode"]].copy()
datecodeuse = datecode[(datecode['date'] <= WindTradingDay_(fromdt=None, todt=enddate)['date'].iloc[-lookfutdays - 1])]
# cal excess return for leave funds (overall and percentile )
eventclass.initdataclass_()
datecode = eventclass.geteventdf_(datecode=datecodeuse, startdate=startdate, enddate=enddate)
eventclass.getpricedata_(idxcode='885001.WI')
eventeret = eventclass.calceventret_(plotTF=True)
eret_ind_leave = eventclass.eretdfall
plt.show()
raise Exception()

# compare the origial data
# eventeret.to_csv("D:\eret.csv")
# leavedatesample = pd.read_excel("D:\stkfundmngerchangeeventdata.xlsx", encoding='ISO-8859-1')
# leavedatesample['leavetdate'] = leavedatesample['leavetdate'].astype(str)
# leavedatesample = leavedatesample.rename(columns={"fundcode": "code", "leavetdate": "date"})
# leavedatesample["ticker_1"] = 1
# datecode['ticker_2'] = 2
# compare = pd.merge(leavedatesample, datecode, on=["code", "date"], how="outer")
# # # raise Exception()
# t1null = compare[compare["ticker_1"].isnull()]
# t2null = compare[compare["ticker_2"].isnull()]
# nulllall = pd.concat([t1null, t2null])
# nulllall.to_csv("D:\diffindatecode.csv")


qeret = eventclass.calqeret_(lookbackdays=lookbackdays, n_group=5)
fig, ax = plt.subplots()
ax = qeret.plot(kind="line", figsize=(20, 16))
# fig = ax.get_figure()
# fig.savefig('AR for mnger departure_10quantile.png')
plt.show()
# raise Exception()
# compare leave funds and all funds
datecodeall = fundlist[["date", "fundcode"]].copy()
datecodeall = eventclass.geteventdf_(datecode=datecodeall, startdate=startdate, enddate=enddate)
eventeretall = eventclass.calceventret_(plotTF=False)
qeretall = eventclass.calqeret_(lookbackdays=lookbackdays, n_group=5)
eventeret_com = eventeret.set_index(["dnum"])
comp_res = pd.concat([qeretall, eventeret_com], axis=1)
comp_res.plot(kind="line", figsize=(20, 16))
plt.show()

# cal weights of leave funds in each fund quantile
eret_ind_all = eventclass.eret_quantle

eret_ind_all_lookback = eret_ind_all[eret_ind_all["dnum"] == -lookbackdays].copy()
eret_ind_leave_lookback = eret_ind_leave[eret_ind_leave["dnum"] == -lookbackdays].copy()
quantilesdf = get_minmax_value_percentile_(eret_ind_all_lookback)
temp = pd.DataFrame()
for i in quantilesdf.index:
    temp[i] = np.where(eret_ind_leave_lookback["eret"].between(quantilesdf.loc[i, "min"], quantilesdf.loc[i, "max"]), i, np.nan)
eret_ind_leave_lookback["rank"] = temp.sum(axis=1)
stat_leave_quanitle = {}
for i in eret_ind_all_lookback["rank"].unique():
    temp1 = eret_ind_leave_lookback[eret_ind_leave_lookback["rank"] == i]
    temp2 = eret_ind_all_lookback[eret_ind_all_lookback["rank"] == i]
    stat_leave_quanitle[i] = [temp1["eret"].count(), temp2["code"].nunique()]
stat_leave_quanitle = pd.DataFrame.from_dict(stat_leave_quanitle).T.reset_index()
stat_leave_quanitle.columns = ["rank", "leave_count", "total_count"]
stat_leave_quanitle["weight_leave"] = stat_leave_quanitle["leave_count"] / stat_leave_quanitle["total_count"]
stat_leave_quanitle = stat_leave_quanitle.sort_values(["rank"])

# raise Exception()

# add netasset factor and track the upgarde/downgrade

fclass.initdataclass_()
sigdata = fclass.getfac_with_fundcode_(periods=mfdates)
sigdata.columns = ['date', 'fundcode', 'sig']
sigdata = pd.merge(mngerdatause, sigdata, on=["date", "fundcode"], how="inner")
turnover = sigdata[["fundcode", 'fundmanagerid', "startdate", "leavedate", "sig"]].copy()
temp = turnover.groupby(["fundmanagerid"])["fundcode"].count()
turnover = pd.merge(turnover, temp, on=['fundmanagerid'], how="inner")
turnover = turnover.rename(columns={"fundcode_x": "fundcode", "fundcode_y": "no_funds"})
turnover_count = turnover[turnover["no_funds"] > 1].sort_values(["fundmanagerid"])

total_overlap = turnover_count.groupby(["fundmanagerid"]).apply(lambda x: cal_overlap_(x))
# raise Exception()
total_overlap = total_overlap[total_overlap["check_overlap"] == 0].drop(columns=["fundmanagerid"]).copy().reset_index()

total_dsig = total_overlap.groupby(["fundmanagerid"]).apply(lambda x: cal_dsig_(x))
total_dsig = total_dsig.sort_values("fundmanagerid")
# raise Exception()
upgrade = total_dsig[total_dsig["dsig"] > 0][["fundcode", "leavedate"]].reset_index()
upgrade = upgrade.rename(columns={"leavedate": "date"})
downgrade = total_dsig[total_dsig["dsig"] < 0][["fundcode", "leavedate"]].reset_index()
downgrade = downgrade.rename(columns={"leavedate": "date"})
pure_leave = turnover[turnover["no_funds"] == 1].sort_values(["fundmanagerid"])
pure_leave = pure_leave[["fundcode", "leavedate"]]
pure_leave = pure_leave.rename(columns={"leavedate": "date"})

# pure_leave.to_csv("D:\\pure_leave.csv")
# upgrade.to_csv("D:\\upgrade.csv")
# downgrade.to_csv("D:\\downgrade.csv")

# raise Exception()
# upgrade
mngerdatause_u = pd.merge(mngerdatause, upgrade, on=["date", "fundcode"], how="inner")
datecode_u = mngerdatause_u[['leavedate', "fundcode"]].copy()
datecodeuse_u = datecode_u[(datecode_u['leavedate'] <= WindTradingDay_(fromdt=None, todt=enddate)['date'].iloc[-lookfutdays - 1])]
datecode_u = eventclass.geteventdf_(datecode=datecodeuse_u, startdate=startdate, enddate=enddate)
eventeret_u = eventclass.calceventret_(plotTF=True)

# downgrade
mngerdatause_d = pd.merge(mngerdatause, downgrade, on=["date", "fundcode"], how="inner")
datecode_d = mngerdatause_d[['leavedate', "fundcode"]].copy()
datecodeuse_d = datecode_d[(datecode_d['leavedate'] <= WindTradingDay_(fromdt=None, todt=enddate)['date'].iloc[-lookfutdays - 1])]
datecode_d = eventclass.geteventdf_(datecode=datecodeuse_d, startdate=startdate, enddate=enddate)
eventeret_d = eventclass.calceventret_(plotTF=True)

# pure_leave
mngerdatause_pure = pd.merge(mngerdatause, pure_leave, on=["date", "fundcode"], how="inner")
datecode_pure = mngerdatause_pure[['leavedate', "fundcode"]].copy()
datecodeuse_pure = datecode_pure[(datecode_pure['leavedate'] <= WindTradingDay_(fromdt=None, todt=enddate)['date'].iloc[-lookfutdays - 1])]
datecode_pure = eventclass.geteventdf_(datecode=datecodeuse_pure, startdate=startdate, enddate=enddate)
eventeret_pure = eventclass.calceventret_(plotTF=True)

# stat for leave manager funds
mngerdata_stat = get_interval_2dates_(mngerdatause, date1="anndate", date2="leavedate")
leave_stat = cal_departure_rate_(mngerdata_stat, fundlist, lookfutdays, startdate, enddate)

# turnover = mngerdatause[["fundcode", 'fundmanagerid', "startdate", "leavedate"]]
# turnover_act = count_mnger_(turnover, groupby="fundmanagerid", countvar="fundcode", newname="no_of_funds")
# # raise Exception()
# turnover_act = turnover.drop_duplicates(subset=["leavedate"], keep=False)
#
# turnover_act = count_mnger_(turnover_act, groupby="fundmanagerid", countvar="leavedate", newname="turnover")
# raise Exception()
#
# turnover_act = turnover_act.sort_values(["turnover"], ascending=False).set_index(['fundmanagerid'])


# dsig=turnover_count.groupby(["fundmanagerid","fundcode"])["sig"].diff()
# turnover_temp=turnover.drop_duplicates(subset=["leavedate"], keep=False)
# turnover["turnover"] = count_mnger_(turnover_temp , groupby="fundmanagerid", countvar="leavedate", newname="turnover")
# temp
# turnover[turnover["turnover"].isnull()]["leavedate"]
# turnover.groupby("fundmanagerid")["leavedate"].count()


#
