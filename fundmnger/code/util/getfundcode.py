# -*- coding: utf-8 -*-

from database.code.windsql.windcustomcode import windcustomcode_
import pandas as pd


def singleton(cls):
    _instances = {}

    def get_instance(*args, **kwargs):
        if cls not in _instances:
            _instances[cls] = cls(*args, **kwargs)
        return _instances[cls]

    return get_instance


@singleton
class getfundcodeclass_:

    def __init__(self):
        """
        Wind在收录基金代码时如果基金在二级市场上市，那么后缀为.SH或者.SZ
        为统一后缀为.OF则构建此方法
        如果一个数据集是从Wind底层数据库直接提取出来的，需要使用此方法对基金后缀进行调整
        """
        self.windcustomcode = windcustomcode_()
        self.periodsuse = None

    def getfac_(self, orgdata=None, date=None):
        if self.periodsuse is not None:
            fundcodesecid = self.windcustomcode.getdata_(date=max(self.periodsuse), sec_typelist=['J'])
        elif date is not None:
            fundcodesecid = self.windcustomcode.getdata_(date=date, sec_typelist=['J'])
        else:
            raise Exception('date and periods are all None!')

        fundcodesecid = fundcodesecid[['s_info_windcode', 'sec_id']].copy()
        fundcodesecid.columns = ['fundcode', 'secid']
        orgdatanames = list(orgdata.columns)
        orgdata = pd.merge(orgdata, fundcodesecid, on=['fundcode'], how='inner')
        orgdata.drop(columns=['fundcode'], inplace=True)
        orgdata = pd.merge(orgdata, fundcodesecid, on=['secid'], how='inner')
        orgdata = orgdata[orgdata['fundcode'].str[-2:] == 'OF'].reset_index(drop=True)
        orgdata.drop(columns=['secid'], inplace=True)
        orgdata = orgdata[orgdatanames]
        return orgdata

    def secid_to_fundcode_(self, orgdata=None, date=None):
        if self.periodsuse is not None:
            fundcodesecid = self.windcustomcode.getdata_(date=max(self.periodsuse), sec_typelist=['J'])
        elif date is not None:
            fundcodesecid = self.windcustomcode.getdata_(date=date, sec_typelist=['J'])
        else:
            raise Exception('date and periods are all None!')

        fundcodesecid = fundcodesecid[['s_info_windcode', 'sec_id']].copy()
        fundcodesecid.columns = ['fundcode', 'secid']
        orgdatanames = list(orgdata.columns)
        orgdatanames[[i for i in range(len(orgdatanames)) if orgdatanames[i] == 'secid'][0]] = 'fundcode'
        orgdata = pd.merge(orgdata, fundcodesecid, on=['secid'], how='inner')
        orgdata = orgdata[orgdata['fundcode'].str[-2:] == 'OF'].reset_index(drop=True)
        orgdata.drop(columns=['secid'], inplace=True)
        orgdata = orgdata[orgdatanames]
        return orgdata
