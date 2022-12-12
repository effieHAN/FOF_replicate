# -*- coding: utf-8 -*-
import pandas as pd
from util.WindTradingDay import WindTradingDay_
import numpy as np
from util.utilfun import tdatesshift_


def cal_dsig_(temp):
    temp["dsig"] = temp.sort_values('startdate', ascending=False)["sig"].diff()
    return temp


def cal_overlap_(temp):
    temp = temp.sort_values("startdate").copy()
    temp["check_overlap"] = np.where((temp["startdate"] <= temp["leavedate"].shift(1)) & (temp["leavedate"] >= temp["leavedate"].shift(1)), 1, 0)
    return temp


def cal_departure_rate_(mngerdata, fundlist, lookfutdays, startdate, enddate):
    yeardata = mngerdata.loc[mngerdata["leavedate"] > startdate,
                             ['fundcode', "anndate", 'fundmanagerid', 'leavedate']].copy()
    yeardata['leaveyear'] = yeardata["leavedate"].str[:4]
    fundlist['annyear'] = fundlist["date"].str[:4]
    stat = pd.DataFrame()
    stat["count_leave"] = \
        yeardata[yeardata["leavedate"] < WindTradingDay_(fromdt=None, todt=enddate)['date'].iloc[-lookfutdays - 1]].groupby("leaveyear")[
            "leavedate"].count()
    stat["count_total"] = fundlist.groupby("annyear")["fundcode"].nunique()
    stat["departure_rate"] = stat["count_leave"] / stat['count_total']
    return stat


def compute_ic_(eret):
    funeret = eret.mean()
    tracking_error = eret.std()
    if tracking_error != 0:
        information_ratio = funeret / tracking_error
    else:
        information_ratio = np.nan
    return information_ratio * np.sqrt(240)


def fund_detail_(subdata):
    subdata = subdata.sort_values("startdate").copy()
    # subdata["next_startdate"] = subdata["startdate"].shift(-1)
    subdata["last_leavedate"] = subdata["date"].shift(1)
    subdata["last_mnger"] = subdata["fundmanagerid"].shift(1)
    subdata = subdata[subdata["last_leavedate"].notna()]
    subdata["ind_turnover"] = np.where((subdata["startdate"].values > tdatesshift_(subdata["last_leavedate"].values, 0)["tdate"]), 1, 0)
    subdata = subdata.reset_index(drop=True)
    return subdata


def mnger_detail_(subdata):
    subdata = subdata.sort_values("startdate").copy()
    subdata["last_fundcode"] = subdata["fundcode"].shift(1)
    # subdata["last_fundcode"] = np.where(subdata["ind_turnover"]==1,subdata["fundcode"].shift(1),subdata["fundcode"].shift(2))

    subdata = subdata.reset_index(drop=True)
    return subdata


#
def count_mnger_(sigdata, groupby=None, countvar=None, newname=None):
    turn_count = sigdata.groupby(groupby)[countvar].count()
    turn_count = pd.DataFrame(turn_count)
    turn_count = turn_count.rename(columns={countvar: newname})
    sigdata = pd.merge(sigdata, turn_count, on=groupby, how="inner")
    return sigdata[newname]


def get_next_mnger_(subdata):
    subdata=subdata.sort_values(["date"]).copy()
    # subdata["kept"]=np.where(subdata["fundmanagerid"]==subdata["fundmanagerid"].shift(-1),0,1)
    # subdata=subdata[subdata["kept"]==1]
    # subdata["last_leavedate"] = subdata["date"].shift(1)
    subdata["next_mnger"] = subdata["fundmanagerid"].shift(-1)
    subdata['next_mnger']
    subdata = subdata.reset_index(drop=True)

    return subdata

def keep_change_(subdata):
    subdata = subdata.sort_values("startdate").copy()
    subdata["kept"] = np.where(subdata["compname"] == subdata["compname"].shift(-1), 0, 1)
    subdata = subdata[subdata["kept"] == 1]
    return subdata

def get_lastcode_(subdata):
    subdata = subdata.sort_values("startdate").copy()
    # subdata["kept"] = np.where(subdata["fundcode"] == subdata["fundcode"].shift(-1), 0, 1)
    # subdata = subdata[subdata["kept"] == 1]
    subdata["last_fundcode"] = subdata["fundcode"].shift(1)
    subdata = subdata.reset_index(drop=True)
    return subdata


def keep_change_for_startdate_(subdata):
    subdata = subdata.sort_values("startdate").copy()
    subdata["kept"] = np.where(subdata["compname"] == subdata["compname"].shift(1), 0, 1)
    subdata = subdata[subdata["kept"] == 1]
    return subdata


def get_mean_alpha_(datecode, alpha):
    res = {}
    datecode = datecode.copy()
    datecode.columns = ["date", "code"]
    for i in datecode.index:
        code = datecode.loc[i, "code"]
        maxdate = datecode.loc[i, "date"]
        mindate = tdatesshift_([maxdate], -240)
        mindate = mindate.loc[0, 'tdate']
        # print(mindate, maxdate)
        datelist = alpha[alpha["fundcode"] == code]["date"].to_list()
        datelistuse = [x for x in datelist if (x <= maxdate) & (x >= mindate)]
        # datelistuse = [x for x in datelist if (x <= maxdate) ]
        temp = alpha[(alpha["fundcode"] == code) & (alpha['date'].isin(datelistuse))]['alpha'].mean()
        res[code] = [maxdate, temp]
    return res
