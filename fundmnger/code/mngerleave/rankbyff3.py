# -*- coding: utf-8 -*-
from FOF.util.getfundcode import getfundcodeclass_
import pandas as pd
from FOF.util.fundmngerutilclass import fundmngerutilclass_
from util.utilfun import get_first_tradingday_month_
from FOF.code.mngerleave.util.util_mnger_leave import get_lastcode_, get_next_mnger_, get_mean_alpha_
from functools import reduce
from util.utilfun import tdatesshift_

mngerclass = fundmngerutilclass_()
fcdclass = getfundcodeclass_()
startdate = '20100104'
enddate = '20220708'
alpha = pd.read_excel(r"D:\打分因子.xlsx")
alpha = alpha[['date', 'fundcode', "alphatmff3"]].copy()
alpha.columns = ['date', "fundcode", 'alpha']
alpha['date'] = alpha['date'].astype(str)
alpha = alpha.sort_values(['date'])
periods = sorted(set(alpha["date"]))
periods = [x for x in periods if (x <= enddate)]
mainmngerdata = mngerclass.getmainmnger_(periods=periods)


# subdata = mainmngerdata[mainmngerdata["fundcode"] == "000021.OF"]

# mainmngerdata = mainmngerdata.drop_duplicates(subset=["fundcode", 'fundmanagerid'], keep="last")
# import numpy as np


def get_last_mnger_(subdata):
    subdata = subdata.sort_values("date").copy()
    # subdata["kept"]=np.where(subdata["fundmanagerid"]==subdata["fundmanagerid"].shift(-1),0,1)
    # subdata=subdata[subdata["kept"]==1].copy()
    subdata = subdata.drop_duplicates(subset=['fundcode', 'fundmanagerid'], keep='last')
    subdata["last_leavedate"] = subdata["leavedate"].shift(1)
    subdata["last_mnger"] = subdata["fundmanagerid"].shift(1)
    # subdata = subdata[subdata["last_leavedate"].notna()]
    subdata = subdata.reset_index(drop=True)
    return subdata


groupbyfund = mainmngerdata.groupby(["fundcode"]).apply(lambda x: get_last_mnger_(x))
groupbyfund = groupbyfund.reset_index(drop=True)


def get_perfundcode_(subdata):
    subdata = subdata.sort_values("startdate").copy()
    subdata["last_fundcode"] = subdata["fundcode"].shift(1)  # exist some fund only started no last mnger
    subdata = subdata.reset_index(drop=True)
    return subdata


groupbymnger = groupbyfund.groupby(["fundmanagerid"]).apply(lambda x: get_perfundcode_(x))
# raise Exception()
groupbymnger = groupbymnger.reset_index(drop=True)

checck = groupbymnger[['date', 'fundcode', 'fundmanagerid', 'startdate', 'leavedate', 'last_leavedate', 'last_mnger', 'last_fundcode']]

no_pervious_1 = groupbymnger[(groupbymnger['last_fundcode'].isnull())].copy()
has_pervious = groupbymnger[(groupbymnger['last_fundcode'].notna())]
no_pervious_1["no_pervious"] = 0.5

has_pervious_copy=has_pervious.rename(columns={'last_fundcode':'fundcode_key','fundmanagerid':'mnger_key'}).copy()
mainmngerdata_copy=mainmngerdata.rename(columns={'fundcode':'fundcode_key','fundmanagerid':'mnger_key'}).copy()

expmorethan1=pd.merge(has_pervious_copy[['fundcode_key',"leavedate",'mnger_key']],mainmngerdata_copy[['date','fundcode_key','mnger_key']],on=['mnger_key','fundcode_key'],how='inner')
expmorethan1['date_min']=tdatesshift_(expmorethan1['leavedate'].to_list(),-240)['tdate']
raise Exception()


expmorethan1[(expmorethan1['fundcode_key']=='000471.OF')&(expmorethan1["mnger_key"]=='00004')]
expmorethan1.groupby(['mnger_key',"fundcode_key"])["date"].count()



def get_ff3_(datecode, alpha):
    ff3 = pd.merge(alpha, datecode, on=['date', 'fundcode'], how='inner')
    return ff3


has_pervious = get_first_tradingday_month_(has_pervious, datenameuse='leavedate').copy()

datecode_an = has_pervious[["date", "fundcode"]].copy()
ff3_an = get_ff3_(datecode_an, alpha)
ff3_an = ff3_an.rename(columns={"alpha": "an_alpha"})

combine = pd.merge(has_pervious, ff3_an, on=['date', 'fundcode'], how='inner')
#
# pd.merge(alpha,datecode_su_pre,on=['date','fundcode'],how='inner')


datecode_su = has_pervious[["date", "last_fundcode"]].copy()
datecode_su.columns = ['date', 'fundcode']
ff3_su = get_ff3_(datecode_su, alpha)
ff3_su = ff3_su.rename(columns={"alpha": "su_alpha", 'fundcode': 'last_fundcode'})

#
combine = pd.merge(combine, ff3_su, on=['date', 'last_fundcode'], how='inner')
# raise Exception()
combine = combine.rename(columns={'date': 'leavedate', 'anndate': 'date'})

better_su = combine[combine["an_alpha"] < combine["su_alpha"]].copy()
better_su["better"] = 1
worse_su = combine[combine["an_alpha"] > combine["su_alpha"]].copy()
worse_su["worse"] = -1

# raise Exception()
dflist = [alpha, worse_su[['date', 'fundcode', 'worse']],
          better_su[['date', 'fundcode', 'better']],
          no_pervious_1[['date', 'fundcode', 'no_pervious']]]
res = reduce(lambda df1, df2: pd.merge(df1, df2, on=['date', 'fundcode'], how="left"), dflist)
res = res.fillna(0)
res["mark"] = res["better"] + res["worse"] + res["no_pervious"]
res = res[['date', 'fundcode', 'alpha', "mark"]]
