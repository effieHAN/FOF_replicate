# -*- coding: utf-8 -*-
"""
Created on Mon Jul 15 13:08:11 2019

@author: liukai4
"""

import pandas as pd
import numpy as np
from FOF.util.getfundcode import getfundcodeclass_
from util.factorclass import factor

"""
由于基金代码在数据库中可能存在历史数据被重刷的情况，因此在保存时只使用Wind的证券id进行保存
在提取数据的时候使用Wind的证券id匹配会当时的基金代码 
"""


class fundfactor(factor):

    def __init__(self, facname=None, facnameuse=None, varnamelist=None):
        super().__init__(facname, facnameuse, varnamelist, factype='fund')
        self.fundcodesecidclass = getfundcodeclass_()
        self.periodsuse = None

    def getfundcode_(self, orgdata=None, date=None):
        if date is None:
            date = max(self.periodsuse)
        orgdata = self.fundcodesecidclass.getfac_(orgdata=orgdata, date=date)
        return orgdata

    def solvefac_(self, fundcodesig, date):
        factorfinal = fundcodesig.copy()
        if ('sig' in factorfinal.columns) and (factorfinal.shape[1] == 2):
            factorfinal = factorfinal[['fundcode', 'sig']].copy()
            factorfinal.columns = ['fundcode', self.facnameuse]
            factorfinal = factorfinal[factorfinal[self.facnameuse].notnull()]
            factorfinal = factorfinal[~(np.isinf(factorfinal[self.facnameuse]))]
            factorfinal.dropna(subset=[self.facnameuse], axis=0, inplace=True)
        factorfinal.reset_index(drop=True, inplace=True)
        # 将基金代码映射到证券代码
        fundcodesecid = self.fundcodesecidclass.windcustomcode.getdata_(date=date, sec_typelist=['J'])
        fundcodesecid = fundcodesecid[['s_info_windcode', 'sec_id']].copy()
        fundcodesecid.columns = ['fundcode', 'secid']
        factorfinal = pd.merge(factorfinal, fundcodesecid, on=['fundcode'], how='inner')
        factorfinal['date'] = date
        factorfinal = factorfinal[['date', 'secid'] + self.varnamelist]
        return factorfinal

    def solvefacperiods_(self, datefundcodesigs):
        periods = sorted(set(datefundcodesigs['date']))
        factorfinal = datefundcodesigs.copy()
        if ('sig' in factorfinal.columns) and (factorfinal.shape[1] == 3):
            factorfinal = factorfinal[['date', 'fundcode', 'sig']].copy()
            factorfinal.columns = ['date', 'fundcode', self.facnameuse]
            # factorfinal[self.facnameuse] = factorfinal[self.facnameuse].astype(float)
            factorfinal = factorfinal[factorfinal[self.facnameuse].notnull()]
            factorfinal = factorfinal[~(np.isinf(factorfinal[self.facnameuse]))]
            factorfinal.dropna(subset=[self.facnameuse], axis=0, inplace=True)
        factorfinal.reset_index(drop=True, inplace=True)
        # 将基金代码映射到证券代码
        fundcodesecid = self.fundcodesecidclass.windcustomcode.getdata_(date=max(periods), sec_typelist=['J'])
        fundcodesecid = fundcodesecid[['s_info_windcode', 'sec_id']].copy()
        fundcodesecid.columns = ['fundcode', 'secid']
        factorfinal = pd.merge(factorfinal, fundcodesecid, on=['fundcode'], how='inner')
        factorfinal = factorfinal[['date', 'secid'] + self.varnamelist]
        factorfinal = factorfinal.sort_values(['date', 'secid'], ascending=True).reset_index(drop=True).copy()
        return factorfinal

    def getfac_(self, periods=None):
        periods = pd.Series(periods)
        if self.local_data is None:
            periodsuse = periods
            factors = pd.DataFrame()
        else:
            facdata = self.local_data.loc[
                self.local_data['date'].isin(periods), ['date', 'secid'] + self.varnamelist]
            periodsuse = periods[~periods.isin(facdata['date'])]
            factors = facdata[facdata['secid'] != 'None'].reset_index(drop=True).copy()
            # factors = facdata.copy()
            factors = factors[['date', 'secid'] + self.varnamelist].copy()
        if len(periodsuse) > 0:
            self.periodsuse = list(periodsuse)
            self.initdataclass_()
            self.local_data_h5 = pd.HDFStore(self.facdatapath, complevel=9, complib='blosc:blosclz')
            factortemp = self.sigconstructperiods_(periods=self.periodsuse)
            try:
                self.local_data = self.local_data_h5[self.facnameuse]
            except KeyError:
                pass
            if self.local_data is None:
                self.appendh5_(self.local_data_h5, factortemp)
                self.local_data = self.local_data_h5[self.facnameuse]
            else:
                if len(periodsuse[~periodsuse.isin(sorted(set(self.local_data['date'])))]) == len(periodsuse):
                    self.appendh5_(self.local_data_h5, factortemp)
                    self.local_data = self.local_data_h5[self.facnameuse]
            self.local_data = self.local_data_h5[self.facnameuse]
            self.local_data_h5.close()
            factortemp = factortemp[['date', 'secid'] + self.varnamelist].copy()
            factors = pd.concat([factors, factortemp], axis=0, ignore_index=True)
        return factors

    def sigconstructperiods_(self, periods=()):
        factortemplist = []
        for date in periods:
            factordt = self.sigconstruct_(date)
            self.appendh5_(self.local_data_h5, factordt)
            # local_data_h5.append(self.facnameuse, factordt, min_itemsize={'fundcode': 40})
            ## self.local_data = pd.concat([self.local_data,factor],axis = 0,ignore_index = True)
            factordt = factordt[['date', 'secid'] + self.varnamelist].copy()
            factortemplist = factortemplist + [factordt]
        factortemp = pd.concat(factortemplist, axis=0, ignore_index=True)
        return factortemp

    def appendh5_(self, h5file, factordt):
        h5file.append(self.facnameuse, factordt, min_itemsize={'secid': 40}, index=False, data_columns=factordt.columns)

    def getfac_with_fundcode_(self, periods=None):
        factors = self.getfac_(periods)
        factors = self.secid2fundcode_(factors, date=max(periods))
        return factors

    def secid2fundcode_(self, sigdata, date):
        fundcodesecid = self.fundcodesecidclass.windcustomcode.getdata_(date=date, sec_typelist=['J'])
        fundcodesecid = fundcodesecid[['s_info_windcode', 'sec_id']].copy()
        fundcodesecid.columns = ['fundcode', 'secid']
        fundcodesecid = fundcodesecid[fundcodesecid['fundcode'].str[-2:] == 'OF'].reset_index(drop=True)

        facnames = list(sigdata.columns)
        factors = pd.merge(sigdata, fundcodesecid, on=['secid'], how='inner')
        factors = factors[['fundcode' if x == 'secid' else x for x in facnames]].copy()
        return factors


    def sigconstruct_(self, date=None):
        return pd.DataFrame()

    def deletefac_(self, periods=None, fromdt=None, todt=None):
        """
        Parameters
        ----------
        periods: all ['yyyymmdd','yyyymmdd']
        """
        if self.local_data is None:
            raise Exception('想要删除的文件不存在!')
        local_data_h5 = pd.HDFStore(self.facdatapath, complevel=9, complib='blosc:blosclz')
        local_data_h5.remove(self.facnameuse)
        if periods != 'all':
            if fromdt is not None and todt is None:
                self.local_data = self.local_data.loc[self.local_data['date'] <= fromdt].reset_index(drop=True)
            elif periods is not None and fromdt is None and todt is None:
                self.local_data = self.local_data.loc[~self.local_data['date'].isin(periods)].reset_index(drop=True)
            else:
                print(periods)
                print(fromdt)
                print(todt)
                raise Exception('periods fromdt todt 参数需要查看')
            self.appendh5_(local_data_h5, self.local_data)
            # local_data_h5.append(self.facnameuse, self.local_data, min_itemsize={'secid': 40}, index=False,
            #                      data_columns=self.local_data.columns)

        local_data_h5.close()
        print('已删除')

    def deletekey_(self):
        if self.local_data is None:
            raise Exception('想要删除的文件不存在!')
        local_data_h5 = pd.HDFStore(self.facdatapath, complevel=9, complib='blosc:blosclz')
        local_data_h5.remove(self.facnameuse)
        local_data_h5.close()
        print('已删除')
