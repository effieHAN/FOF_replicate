

#
# # calculate leave-mnger's past 240days ic
# datecode = turnoveruse[['date', "fundcode"]].copy()
# datecodeuse = datecode[(datecode['date'] <= WindTradingDay_(fromdt=None, todt=enddate)['date'].iloc[-lookfutdays - 1])]
# icclass.initdataclass_()
# datecode = icclass.geteventdf_(datecode=datecodeuse, startdate=startdate, enddate=enddate)
# icclass.getpricedata_(idxcode='885001.WI')
# inforatio_leavefund = icclass.cal_ic_()
# #
# datecodesbench=datecode.copy().rename(columns={"date":"benchdate","code":"fundcode"})
# successor=pd.merge(turnover,datecodesbench,on=["fundcode"],how="inner")





#
# meta=pd.merge(groupbyfund[["fundcode","fundmanagerid","last_leavedate","startdate"]],
#               groupbymnger[["fundmanagerid","fundcode","last_fundcode"]],on=["fundcode",'fundmanagerid'],how="inner")


raise Exception()

count_temp = groupbymnger.groupby(['fundmanagerid'])["fundcode"].count()
# count_temp = pd.DataFrame(count_temp).reset_index()
groupbyfund = pd.merge(groupbyfund, count_temp, on=["fundmanagerid"], how="inner")
groupbyfund = groupbyfund.rename(columns={"fundcode_x": "fundcode", "fundcode_y": "no_funds_per_mnger"})
# case1  successor has no previous career to calculate ic
# case12  independently successor decide and he has previous career that can cal ic
# case 22 helped successor and has previous career that can cal ic

case1 = groupbyfund[(groupbyfund["no_funds_per_mnger"] == 1)]
case2 = groupbyfund[groupbyfund["no_funds_per_mnger"] > 1]
case12 = groupbyfund[((groupbyfund["no_funds_per_mnger"] > 1) & (groupbyfund["ind_turnover"] == 1))]
case22 = groupbyfund[((groupbyfund["no_funds_per_mnger"] > 1) &(groupbyfund["no_funds_per_mnger"] > 1))]
raise Exception()

case12[['fundcode', 'fundmanagerid', 'startdate','leavedate','date','sig','last_leavedate',]].set_index(['fundmanagerid',"fundcode"])
case12[case12["fundmanagerid"]=="00775"]




# for successor with help decide whether he has previous career that can cal ic
# case21 helped successor and no previous

# ind_all_no = ind_all[ind_all["ind_turnover"] == 0]
# ind_all_no[["fundcode","fundmanagerid","startdate",'leavedate']].set_index(['fundcode',"fundmanagerid"])

# career_before=ind_all_no[ind_all_no["startdate"]<ind_all_no["last_leavedate"]]
# has_previous_career=pd.merge(ind_all_no[["fundmanagerid","startdate"]]
raise Exception()

# calculate the ic of successor's past 240days
datecode = turnoveruse[['leavedate', "fundcode"]].copy()
datecodeuse = datecode[(datecode['leavedate'] <= WindTradingDay_(fromdt=None, todt=enddate)['date'].iloc[-lookfutdays - 1])]
icclass.initdataclass_()
datecode = icclass.geteventdf_(datecode=datecodeuse, startdate=startdate, enddate=enddate)
icclass.getpricedata_(idxcode='885001.WI')
inforatio_first = icclass.cal_ic()
# no_fundret=icclass.eretdfall[icclass.eretdfall["fundret"].isnull()]["code"].unique()

#
#
#
# datecode = mngerdatause[['leavedate', "fundcode"]].copy()
# datecodeuse = datecode[(datecode['leavedate'] <= WindTradingDay_(fromdt=None, todt=enddate)['date'].iloc[-lookfutdays - 1])]
# # cal excess return for leave funds (overall and percentile )
# eventclass.initdataclass_()
# datecode = eventclass.geteventdf_(datecode=datecodeuse, startdate=startdate, enddate=enddate)
# eventclass.getpricedata_(idxcode='885001.WI')
# eventeret = eventclass.calceventret_(plotTF=True)
# eret_ind_leave = eventclass.eretdfall
# plt.show()
#
# qeret = eventclass.calqeret_(lookbackdays=lookbackdays, n_group=5)
# # fig, ax = plt.subplots()
# # ax = qeret.plot(kind="line", figsize=(20, 16))
# # # fig = ax.get_figure()
# # # fig.savefig('AR for mnger departure_10quantile.png')
# # plt.show()
#
# # compare leave funds and all funds
# datecodeall = fundlist[["date", "fundcode"]].copy()
# datecodeall = eventclass.geteventdf_(datecode=datecodeall, startdate=startdate, enddate=enddate)
# eventeretall = eventclass.calceventret_(plotTF=False)
# qeretall = eventclass.calqeret_(lookbackdays=lookbackdays, n_group=5)
# eventeret_com = eventeret.set_index(["dnum"])
# comp_res = pd.concat([qeretall, eventeret_com], axis=1)
# comp_res.plot(kind="line", figsize=(20, 16))
# plt.show()
#
# # cal weights of leave funds in each fund quantile
# eret_ind_all = eventclass.eret_quantle
#
# eret_ind_all_lookback = eret_ind_all[eret_ind_all["dnum"] == -lookbackdays].copy()
# eret_ind_leave_lookback = eret_ind_leave[eret_ind_leave["dnum"] == -lookbackdays].copy()
# quantilesdf = get_minmax_value_percentile_(eret_ind_all_lookback)
# temp = pd.DataFrame()
# for i in quantilesdf.index:
#     temp[i] = np.where(eret_ind_leave_lookback["eret"].between(quantilesdf.loc[i, "min"], quantilesdf.loc[i, "max"]), i, np.nan)
# eret_ind_leave_lookback["rank"] = temp.sum(axis=1)
# stat_leave_quanitle = {}
# for i in eret_ind_all_lookback["rank"].unique():
#     temp1 = eret_ind_leave_lookback[eret_ind_leave_lookback["rank"] == i]
#     temp2 = eret_ind_all_lookback[eret_ind_all_lookback["rank"] == i]
#     stat_leave_quanitle[i] = [temp1["eret"].count(), temp2["code"].nunique()]
# stat_leave_quanitle = pd.DataFrame.from_dict(stat_leave_quanitle).T.reset_index()
# stat_leave_quanitle.columns = ["rank", "leave_count", "total_count"]
# stat_leave_quanitle["weight_leave"] = stat_leave_quanitle["leave_count"] / stat_leave_quanitle["total_count"]
# stat_leave_quanitle = stat_leave_quanitle.sort_values(["rank"])
#
# # raise Exception()
#
# # add netasset factor and track the upgarde/downgrade
#
# fclass.initdataclass_()
# sigdata = fclass.getfac_with_fundcode_(periods=mfdates)
# sigdata.columns = ['date', 'fundcode', 'sig']
# sigdata = pd.merge(mngerdatause, sigdata, on=["date", "fundcode"], how="inner")
# turnover = sigdata[["fundcode", 'fundmanagerid', "startdate", "leavedate", "sig"]].copy()
# temp = turnover.groupby(["fundmanagerid"])["fundcode"].count()
# turnover = pd.merge(turnover, temp, on=['fundmanagerid'], how="inner")
# turnover = turnover.rename(columns={"fundcode_x": "fundcode", "fundcode_y": "no_funds"})
# turnover_count = turnover[turnover["no_funds"] > 1].sort_values(["fundmanagerid"])
#
# total_overlap = turnover_count.groupby(["fundmanagerid"]).apply(lambda x: cal_overlap_(x))
# raise Exception()
# total_overlap = total_overlap[total_overlap["check_overlap"] == 0].drop(columns=["fundmanagerid"]).copy().reset_index()
#
# total_dsig = total_overlap.groupby(["fundmanagerid"]).apply(lambda x: cal_dsig_(x))
# total_dsig = total_dsig.sort_values("fundmanagerid")
# # raise Exception()
# upgrade = total_dsig[total_dsig["dsig"] > 0][["fundcode", "leavedate"]].reset_index()
# upgrade = upgrade.rename(columns={"leavedate": "date"})
# downgrade = total_dsig[total_dsig["dsig"] < 0][["fundcode", "leavedate"]].reset_index()
# downgrade = downgrade.rename(columns={"leavedate": "date"})
# pure_leave = turnover[turnover["no_funds"] == 1].sort_values(["fundmanagerid"])
# pure_leave = pure_leave[["fundcode", "leavedate"]]
# pure_leave = pure_leave.rename(columns={"leavedate": "date"})
#
#
# # stat for leave manager funds
# mngerdata_stat = get_interval_2dates_(mngerdatause, date1="anndate", date2="leavedate")
# leave_stat = cal_departure_rate_(mngerdata_stat, fundlist, lookfutdays, startdate, enddate)







# raise Exception()
mngerdata = mngerclass.getdata_(fromdt=startdate, todt=enddate)
mngerdata = mngerdata[mngerdata['leavedate'] != 'nan']
# mngerdata = get_first_tradingday_month_(mngerdata,datename="leavedate")
eventdate=sorted(set(mngerdata['leavedate']))

fundlist = fundlistclass.getfac_with_fundcode_(eventdate)
fundlist = fundlist[fundlist['fundtypecode'].isin(['2001010101', '2001010201', '2001010204']) &
                    (fundlist['pos0'] > 0.7) &
                    (fundlist['pos1'] > 0.7) &
                    (fundlist['pos2'] > 0.7) &
                    (fundlist['pos3'] > 0.7) &
                    (fundlist['listdays'] > (lookbackdays + 180))].reset_index(drop=True).copy()
raise Exception()
fundlist=fundlist.rename(columns={"date":"mfdate"})
mngerdatause=pd.merge(mngerdata,fundlist,on=["mfdate","fundcode"],how="inner")

datecode = mngerdatause[['leavedate', "fundcode"]].copy()
datecodeuse = datecode[(datecode['leavedate'] <= WindTradingDay_(fromdt=None, todt=enddate)['date'].iloc[-lookfutdays - 1])]




fig, ax = plt.subplots()
eventeret = eventeret.set_index(["dnum"])
ax = eventeret["eret"].plot(kind="bar", figsize=(20, 16))
fig = ax.get_figure()
fig.savefig('AR for mnger departure_overall.png')
plt.show()


















# from database.code.windsql.chinamutualfundnav import chinamutualfundnav_
# from FOF.util.getfundcode import getfundcodeclass_
# from database.code.windsql.chinamutualfundsector import chinamutualfundsector_
# from database.code.windsql.chinamutualfundassetportfolio import chinamutualfundassetportfolio_
# from functools import reduce
# from util.utilfun import cal_benchmark_return
# from util.utilfun import cal_fund_return
# import pandas as pd
#
# navclass = chinamutualfundnav_()  # initialize  with ()
# fcdclass = getfundcodeclass_()
# sectorclass = chinamutualfundsector_()
# assetclass = chinamutualfundassetportfolio_()
# # self.updatedata_()
#
# # In[]
# # unify the fund appendix(sh/sz to OF.)
# navdata = navclass.getdata_(todt="20221010")
# navdata = fcdclass.getfac_(orgdata=navdata, date='20221010')
#
# # get active/stock mixed-stock flexiable funds
# sectordata = sectorclass.getdata_(varnames=["fundtypecode"], todt="20221010")
# active_fund = sectordata[sectordata["fundtypecode"].isin(["2001010101", "2001010201", "2001010204"])]
#
# # get stkpos>70%
# rpdates = ["20210930", "20211231", "20220331", "20220630"]
# assetdata = assetclass.getdata_(varnames=['netasset', 'stkpos'], rptdates=rpdates)
# # assetdata = assetdata.sort_values(['fundcode', 'rptdate', 'anndate'])
# assetdata = assetdata[assetdata["stkpos"] >= 70]
# assetdatause = assetdata.groupby("fundcode").sum()
# assetdatause = assetdatause[assetdatause["stkpos"] >= 280]
# stkpos_ok_fund = assetdatause.reset_index()
#
# # get perpared data
# vaild_fund = reduce(lambda x, y: pd.merge(x, y, on=['fundcode'], how='inner'),
#                     [active_fund, stkpos_ok_fund, navdata])
# # vaild_fund.columns
# vaild_fund = vaild_fund[['fundcode', 'date', 'navadj', 'netasset']]
# # len(vaild_fund["fundcode"].unique())
#

# -*- coding: utf-8 -*-
"""
Created on Mon Jul 15 13:08:11 2019


"""
import numpy as np
import pandas as pd
from functools import reduce
# from datetime import datetime, timedelta
from util.utilfun import *
# from util.utilfun import get_interval
# from util.utilfun import calculate_return_index
# from util.utilfun import tdatesshift_
from FOF.util.getfundcode import getfundcodeclass_
from database.code.windsql.chinamutualfundassetportfolio import chinamutualfundassetportfolio_
from database.code.windsql.chinamutualfundmanager import chinamutualfundmanager_
from database.code.windsql.chinamutualfundnav import chinamutualfundnav_
from database.code.windsql.chinamutualfundsector import chinamutualfundsector_
from database.code.windsql.cmfindexeod import cmfindexeod_

mngerclass = chinamutualfundmanager_()
navclass = chinamutualfundnav_()  # initialize  with ()
fcdclass = getfundcodeclass_()
sectorclass = chinamutualfundsector_()
assetclass = chinamutualfundassetportfolio_()
# In[]
startdate = '20100104'
enddate = '20221010'
lookbacktdays = 120
lookfuttdays = 60

# get test interval
mngerdata = mngerclass.getdata_(todt=enddate)
mngerdata.loc[mngerdata['leavedate'] == 'nan', 'leavedate'] = enddate

leavedates = sorted(set(mngerdata['leavedate']))  # return a list

datemaplag = tdatesshift_(nperiods=leavedates, Ntdays=-120)  # that's a dataframe
datemaplag.columns = ['leavedate', 'teststartdate']
datemapfut = tdatesshift_(nperiods=leavedates, Ntdays=60)
datemapfut.columns = ['leavedate', 'testenddate']

mngerdatause = reduce(lambda x, y: pd.merge(x, y, on=['leavedate'], how='inner'),
                      [mngerdata, datemaplag, datemapfut])
mngerdatause = mngerdatause[mngerdatause['testenddate'] < enddate]
mngerdatause = mngerdatause[['fundcode', 'teststartdate', 'testenddate', 'leavedate']]
mngerdatause["teststartdate"].unique()
mngerdatause["teststartdate"].astype(int).min()




# calculate the AR in window interval

vaild_fund["within_gap"] = np.where((vaild_fund["date"].astype(int) >= vaild_fund["teststartdate"].astype(int)) & (
        vaild_fund["date"].astype(int) <= vaild_fund["testenddate"].astype(int)), 1, np.nan)
vaild_fund = vaild_fund[vaild_fund["within_gap"].notna()]

vaild_fund["logret"] = vaild_fund.groupby("fundcode")['navadj'].apply(lambda x: np.log(x) - np.log(x.shift(1)))

vaild_fund["teststartdate"].min()
# deal with benchmark




benchmarkclass = cmfindexeod_()
benchmarkdata = benchmarkclass.getdata_()
benchmarkdata = calculate_return_index(benchmarkdata)
benchmarkdata = benchmarkdata[["date", 'index_ret']]

vaild_return = pd.merge(vaild_fund, benchmarkdata, on=["date"], how="inner")
vaild_return["AR"] = vaild_return["logret"] - vaild_return['index_ret']
vaild_return = vaild_return[vaild_return["AR"].notna()]
vaild_return[vaild_return["fundcode"] == '000011.OF']
#

vaild_ready = get_interval(vaild_return)
# vaild_ready.columns
vaild_ready = vaild_ready[["rank", "logret", 'index_ret', 'AR', "gap"]]
ar_aveg = vaild_ready.groupby("gap").mean()
ar_aveg = ar_aveg.set_index("rank")

import matplotlib.pyplot as plt

fig, ax = plt.subplots()
fig=ar_aveg["AR"].plot(kind="bar", figsize=(20, 16))
plt.title("AR for manager depature")
# plt.xticks(ar_aveg.index.values)
plt.show()



##select the fund
# In[]
# unify the fund appendix(sh/sz to OF.)
navdata = navclass.getdata_(todt="20221010")
navdata = fcdclass.getfac_(orgdata=navdata, date=enddate)

# get active/stock mixed-stock flexiable funds
sectordata = sectorclass.getdata_(varnames=["fundtypecode"], todt="20221010")
active_fund = sectordata[sectordata["fundtypecode"].isin(["2001010101", "2001010201", "2001010204"])]
vaild_almost = pd.merge(active_fund, navdata, on=['fundcode'], how='inner')

# len(vaild_almost["fundcode"].unique())

# get stkpos>70%
rpdates = ["20210930", "20211231", "20220331", "20220630"]
assetdata = assetclass.getdata_(varnames=['stkpos'], rptdates=rpdates)
assetdata = assetdata[assetdata["stkpos"] >= 70]
assetdatause = assetdata.groupby("fundcode").sum()
assetdatause = assetdatause[assetdatause["stkpos"] >= 280]
stkpos_ok_fund = assetdatause.reset_index()
# # get perpared data through stkpos and type
vaild_fund = reduce(lambda x, y: pd.merge(x, y, on=['fundcode'], how='inner'),
                    [mngerdatause, vaild_almost, stkpos_ok_fund])
vaild_fund = vaild_fund[['fundcode', 'date', 'navadj', 'leavedate', "teststartdate", "testenddate"]]


#
#
#
#
# from database.code.windsql.chinamutualfundmanager import chinamutualfundmanager_
# mngerclass=chinamutualfundmanager_()
# startdate="20210101"
# enddate="20220101"
# mngerdata=mngerclass.getdata_(fromdt=startdate,todt=enddate)
# mngerdata.loc[mngerdata['leavedate'] == 'nan', 'leavedate'] = enddate
# datecode = mngerdata[['leavedate',"fundcode"]].copy()
# eventclass=calceventretclass_()
#
# eventclass.initdataclass_()
# eventclass.clean_priceclass(startdate,enddate)
# datecode=eventclass.geteventdf_(datecode)
# eventclass.getpricedata_()
# eventret=eventclass.calceventret_(datecode=datecode, plotTF=False)
#
# eventclass.datecodeprice.head()
# # eventclass.datecodetemp.index.duplicated()