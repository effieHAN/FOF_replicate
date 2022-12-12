# # -*- coding: utf-8 -*-
# sth not ok the leavedate mathch and others
from FOF.util.getfundcode import getfundcodeclass_
import pandas as pd
from FOF.util.fundmngerutilclass import fundmngerutilclass_
from util.utilfun import get_first_tradingday_month_
from FOF.code.mngerleave.util.util_mnger_leave import get_lastcode_, get_next_mnger_, get_mean_alpha_
from functools import reduce

mngerclass = fundmngerutilclass_()
fcdclass = getfundcodeclass_()
startdate = '20100104'
enddate = '20220708'
alpha = pd.read_excel(r"D:\PycharmProjects\proj\report\基金经理特质与基金业绩\主动股基打分明细.xlsx")
alpha = alpha[['月份', '基金代码', "综合打分"]].copy()
alpha.columns = ['date', "fundcode", 'alpha']
alpha['date'] = alpha['date'].astype(str)
alpha = get_first_tradingday_month_(alpha, datenameuse='date')
alpha = alpha[['date', 'fundcode', 'alpha']]
periods = sorted(set(alpha["date"]))
periods = [x for x in periods if (x <= enddate)]
mainmngerdata = mngerclass.getmainmnger_(periods=periods)
sample = mainmngerdata[mainmngerdata["fundcode"] == "000021.OF"]
sample[['date', 'fundcode', 'fundmanager', 'fundmanagerid', 'startdate', 'leavedate']]
# mainmngerdata = mainmngerdata.drop_duplicates(subset=["fundcode", 'fundmanagerid'], keep="last")
raise Exception()
# subdata=mainmngerdata[mainmngerdata["fundcode"]=="000021.OF"].copy()
groupbyfund = mainmngerdata.groupby(["fundcode"]).apply(lambda x: get_next_mnger_(x))
groupbyfund = groupbyfund.reset_index(drop=True)
no_change = groupbyfund[groupbyfund["next_mnger"].isnull()].copy()
no_change["nochange"] = -0.5

groupbyfund = groupbyfund[groupbyfund["next_mnger"].notna()]
groupbyfund = groupbyfund.rename(columns={"next_mnger": "mnger_key", "date": "date_key"})

# subdata=mainmngerdata[mainmngerdata['fundmanagerid']=="JR14E3D2C"].copy()
groupbymnger = mainmngerdata.groupby(["fundmanagerid"]).apply(lambda x: get_lastcode_(x))
groupbymnger = groupbymnger.reset_index(drop=True)
groupbymnger = groupbymnger.drop_duplicates(subset=['startdate', 'fundmanagerid'], keep='last')
groupbymnger = groupbymnger.rename(columns={"fundmanagerid": "mnger_key", "date": "date_key"})

allinfo = pd.merge(groupbyfund[["date_key", "fundcode", "fundmanagerid", "mnger_key"]],
                   groupbymnger[["mnger_key", "date_key", "last_fundcode"]], on=["mnger_key", "date_key"], how="inner")
allinfo = allinfo.rename(columns={"mnger_key": 'next_mnger', "date_key": "date"})
# raise Exception()

no_pervious = allinfo[(allinfo['last_fundcode'].isnull())].copy()
has_pervious = allinfo[(allinfo['last_fundcode'].notna())]
no_pervious["no_pervious"] = 0.5

datecode_an = has_pervious[["date", "fundcode"]]
average_alpha_an = get_mean_alpha_(datecode_an, alpha)
average_alpha_an = pd.DataFrame.from_dict(average_alpha_an).T
average_alpha_an = average_alpha_an.reset_index()
average_alpha_an.columns = ["fundcode", "date", "an_alpha"]
combine = pd.merge(has_pervious, average_alpha_an, on=["fundcode", "date"], how="inner")
datecode_su = has_pervious[["date", "last_fundcode"]]
average_alpha_su = get_mean_alpha_(datecode_su, alpha)
average_alpha_su = pd.DataFrame.from_dict(average_alpha_su).T
average_alpha_su = average_alpha_su.reset_index()
average_alpha_su.columns = ["code_key", "date", "su_alpha"]
# raise Exception()
combine = combine.rename(columns={"last_fundcode": "code_key"})
combine = pd.merge(combine, average_alpha_su, on=["code_key", "date"], how="inner")
# raise Exception()

better_su = combine[combine["an_alpha"] < combine["su_alpha"]].copy()
better_su["better"] = 1
worse_su = combine[combine["an_alpha"] > combine["su_alpha"]].copy()
worse_su["worse"] = -1

dflist = [alpha, worse_su[['date', 'fundcode', 'worse']],
          better_su[['date', 'fundcode', 'better']],
          no_pervious[['date', 'fundcode', 'no_pervious']],
          no_change[['date', 'fundcode', 'nochange']]]
res = reduce(lambda df1, df2: pd.merge(df1, df2, on=['date', 'fundcode'], how="left"), dflist)
res = res.fillna(0)
res["mark"] = res["better"] + res["worse"] + res["no_pervious"] + res["nochange"]
res = res[['date', 'fundcode', 'alpha', "mark"]]

datecode_try = alpha[["date", 'fundcode']].head(10)
alpha_try = get_mean_alpha_(datecode_try, alpha)

date1 = has_pervious[['date', 'fundcode']].copy()
date1['ticker1'] = 1
date2 = alpha[['date', 'fundcode']].copy()
date2['ticker2'] = 2

alldate = pd.merge(date1, date2, on=['date', 'fundcode'], how='outer')
alldate[alldate['ticker2'].isnull()]
