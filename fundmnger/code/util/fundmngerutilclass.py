# -*- coding: utf-8 -*-
"""
@author: liukai
"""

import pandas as pd
from FOF.util.getfundcode import getfundcodeclass_
from database.code.windsql.chinamutualfundmanager import chinamutualfundmanager_


def singleton(cls):
    _instances = {}

    def get_instance(*args, **kwargs):
        if cls not in _instances:
            _instances[cls] = cls(*args, **kwargs)
        return _instances[cls]

    return get_instance


@singleton
class fundmngerutilclass_:

    def __init__(self):
        self.fundmngerclass = chinamutualfundmanager_()
        self.getfundcodeclass = getfundcodeclass_()
        self.periodsuse = None

    def getfundmngerinfo_(self, periods=None, maxdate=None):
        if periods is not None and maxdate is None:
            mngernamelist = [self.fundmngerclass.getdata_(date=x) for x in periods]
            # print("mngernamelsit",mngernamelist)
            mngername = pd.concat(mngernamelist, axis=0, ignore_index=True)
            mngername = self.getfundcodeclass.getfac_(mngername, max(periods))
        elif periods is None and maxdate is not None:
            mngername = self.fundmngerclass.getdata_(maxdate=maxdate)
            mngername = self.getfundcodeclass.getfac_(mngername, date=maxdate)
        else:
            raise Exception('params are illegal!')
        return mngername

    def getfundmngerjobdays_(self, periods):
        fundmngerorgall = self.fundmngerclass.getdata_(maxdate=max(periods))
        mngerjobdayslist = []
        for dt in periods:
            # print("dt",dt)
            fundmngerorgdt = fundmngerorgall.copy()
            # fundmngerorgdt['leavedateorg'] = fundmngerorgdt['leavedate']
            fundmngerorgdt.loc[fundmngerorgdt['leavedate'] == 'nan', 'leavedate'] = dt
            fundmngerorgdt.loc[fundmngerorgdt['leavedate'] > dt, 'leavedate'] = dt

            fundmngerorg = self.fundmngerclass.getdata_(date=dt)
            fundmngerorg['leavedateorg'] = fundmngerorg['leavedate']
            fundmngerorg.loc[fundmngerorg['leavedate'] == 'nan', 'leavedate'] = dt
            fundmngerorg.loc[fundmngerorg['leavedate'] > dt, 'leavedate'] = dt
            fundmngerorg = fundmngerorg[fundmngerorg['startdate'] != 'nan'].reset_index(drop=True)
            fundmngerorg = fundmngerorg[fundmngerorg['startdate'] <= fundmngerorg['leavedate']].reset_index(drop=True)
            fundmngerorg['fundmngdays'] = pd.to_datetime(fundmngerorg['date']) - pd.to_datetime(fundmngerorg['startdate'])
            fundmngerorg['fundmngdays'] = fundmngerorg['fundmngdays'].dt.days

            fundmngerorgdtuse = fundmngerorgdt[fundmngerorgdt['fundmanagerid'].isin(fundmngerorg['fundmanagerid'])]
            fundmnger = fundmngerorgdtuse[['fundmanager', 'fundmanagerid', 'startdate', 'leavedate']].copy()
            fundmnger = fundmnger.drop_duplicates(['fundmanagerid', 'startdate', 'leavedate']).reset_index(drop=True)
            fundmnger = fundmnger.sort_values(['fundmanagerid', 'startdate', 'leavedate']).reset_index(drop=True)
            fundmnger = fundmnger.drop_duplicates(['fundmanagerid', 'startdate', 'leavedate']).reset_index(drop=True)
            fundmnger['leavedateint'] = fundmnger['leavedate'].astype(int)
            fundmnger['leavedatemax'] = fundmnger.groupby(['fundmanager', 'fundmanagerid'])['leavedateint'].cummax()
            fundmnger = fundmnger.drop_duplicates(['fundmanagerid', 'leavedatemax'], keep='first').reset_index(drop=True)
            fundmnger = fundmnger.sort_values(['fundmanagerid', 'startdate', 'leavedate']).reset_index(drop=True)
            # fundmnger = fundmnger.drop(columns=['leavedateint', 'leavedatemax'])
            fundmnger['leavedatelag'] = fundmnger.groupby(['fundmanager', 'fundmanagerid'])['leavedate'].shift(1)
            fundmnger.loc[fundmnger['leavedatelag'].isnull(), 'leavedatelag'] = \
                fundmnger.loc[fundmnger['leavedatelag'].isnull(), 'startdate']
            fundmnger.loc[fundmnger['leavedatelag'] > fundmnger['startdate'], 'startdate'] = \
                fundmnger.loc[fundmnger['leavedatelag'] > fundmnger['startdate'], 'leavedatelag']

            fundmnger['jobdays'] = pd.to_datetime(fundmnger['leavedate']) - pd.to_datetime(fundmnger['startdate'])
            fundmnger['jobdays'] = fundmnger['jobdays'].dt.days

            mngerjobdays = fundmnger.groupby(['fundmanagerid'])['jobdays'].sum().reset_index()
            mngerjobdays.columns = ['fundmanagerid', 'jobdays']

            mngerstartdatemin = fundmngerorgdt.groupby(['fundmanagerid'])['startdate'].min()
            mngerleavedatemax = fundmngerorgdt.groupby(['fundmanagerid'])['leavedate'].max()
            mngerjoballdays = pd.to_datetime(mngerleavedatemax) - pd.to_datetime(mngerstartdatemin)
            mngerjoballdays = mngerjoballdays.dt.days.reset_index()
            mngerjoballdays.columns = ['fundmanagerid', 'joballdays']

            """
            fundmngdays: 基金经理管理本基金自然日
            jobdays: 基金经理管理基金累计自然日（不算空档期）
            joballdays: 自基金经理管理第一只产品到现在累计自然日（算上周期内的空档期）
            """
            mngerjobdaystemp = pd.merge(fundmngerorg, mngerjobdays, on=['fundmanagerid'], how='inner')
            mngerjobdaystemp = pd.merge(mngerjobdaystemp, mngerjoballdays, on=['fundmanagerid'], how='inner')
            mngerjobdaystemp = mngerjobdaystemp[['date', 'fundcode', 'fundmanager', 'fundmanagerid', 'startdate', 'leavedateorg',
                                                 'fundmngdays', 'jobdays', 'joballdays']].copy()
            mngerjobdaystemp.columns = ['date', 'fundcode', 'fundmanager', 'fundmanagerid', 'startdate', 'leavedate',
                                        'fundmngdays', 'jobdays', 'joballdays']
            mngerjobdayslist = mngerjobdayslist + [mngerjobdaystemp]
        mngerjobdays = pd.concat(mngerjobdayslist, axis=0, ignore_index=True)
        mngerjobdays = self.getfundcodeclass.getfac_(mngerjobdays, max(periods))
        return mngerjobdays

    def getmainmnger_(self, periods):
        mngerjobdays = self.getfundmngerjobdays_(periods=periods)
        # print(mngerjobdays)
        fundmngerjobdays = mngerjobdays.sort_values(['date', 'fundcode', 'fundmngdays', 'jobdays', 'joballdays'],
                                                    ascending=[True, True, False, False, False]).reset_index(drop=True).copy()
        fundmainmngerjobdays = fundmngerjobdays.drop_duplicates(subset=['date', 'fundcode']).reset_index(drop=True).copy()
        return fundmainmngerjobdays

    # def
# #
# #
# if __name__ == '__main__':
#     self = fundmngerutilclass_()
#     aa = self.getmainmnger_(periods=['20211010',"20220101"])
