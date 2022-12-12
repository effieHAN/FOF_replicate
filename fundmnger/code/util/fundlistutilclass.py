# -*- coding: utf-8 -*-
"""
Created on Sun Mar  8 09:20:48 2020

@author: liukai
"""

import datetime
import numpy as np
import pandas as pd
from FOF.util.factorclass import fundfactor
from database.code.windsql.chinagradingfund import chinagradingfund_
from database.code.windsql.chinamutualfundsector import chinamutualfundsector_
from database.code.windsql.cmfdesc import cmfdesc_
from database.code.windsql.chinamutualfundassetportfolio import chinamutualfundassetportfolio_
from database.code.windsql.cfundpchredm import cfundpchredm_
from database.code.windsql.ashareindustriescode import ashareindustriescode_


# In[] 构造初始基金池
class fundlistutilclass_(fundfactor):
    def __init__(self, facnameuse):
        """
        facnameuse: allstkfund.\n
        ropenticker: 定期开放型基金
        closefundticker：封闭式运作基金
        opencloseticker：续存
        """
        super().__init__(facnameuse + 'list', facnameuse,
                         varnamelist=('rptdate', 'fundtypecode1', 'fundtypename1',
                                      'fundtypecode2', 'fundtypename2',
                                      'fundtypecode3', 'fundtypename3',
                                      'pos0', 'pos1', 'pos2', 'pos3',
                                      'apos0', 'apos1', 'apos2', 'apos3',
                                      'bondpos0', 'bondpos1', 'bondpos2', 'bondpos3',
                                      'cbpos0', 'cbpos1', 'cbpos2', 'cbpos3', 'minholdingprd',
                                      'ropenticker', 'closefundticker', 'opencloseticker',
                                      'listdays', 'listdate', 'entrydateuse'))
        # 可能会用到的基础数据
        self.cmfdescclass = None
        self.cgfclass = None
        self.cmfsectorclass = None
        self.fap = None
        self.cfundpchredmclass = None
        self.typenameclass = None

    def initdataclass_(self):
        self.cmfdescclass = cmfdesc_()
        self.cgfclass = chinagradingfund_()
        self.cmfsectorclass = chinamutualfundsector_()
        self.fap = chinamutualfundassetportfolio_()
        self.cfundpchredmclass = cfundpchredm_()
        self.typenameclass = ashareindustriescode_()

    def appendh5_(self, h5file, factordt):
        h5file.append(self.facnameuse, factordt,
                      min_itemsize={'secid': 40, 'fundtypename1': 50, 'fundtypename2': 50, 'fundtypename3': 50},
                      index=False, data_columns=factordt.columns)

    def getfapdt_(self, date=None):
        fapdt = self.fap.getdata_(varnames=['hkstkval', 'stkpos', 'hkstkpos', 'bondpos', 'covertbondpos'], todt=date)
        fapdt = fapdt[fapdt['anndate'] < date].reset_index(drop=True)
        return fapdt

    def sigconstruct_(self, date=None):
        # print(date)
        # 1. 获取当天存在的全部初始基金
        fundall = self.cmfdescclass.getdata_(varnames=['fundfullname', 'fundname', 'isinitial', 'listdate',
                                                       'fundmaturitydate'], todt=date)
        # 2. 同类型基金只保留A类基金
        finit = fundall[(fundall['isinitial'] == 1) &
                        (((fundall['fundmaturitydate'] >= date) & (fundall['fundmaturitydate'] != 'None')) |
                         (fundall['fundmaturitydate'] == 'None'))].reset_index(drop=True)
        fundsector = self.cmfsectorclass.getdata_(date=date)
        typename = self.typenameclass.getdata_()
        typename = typename[['indcode', 'indname', 'levelnum']].copy()
        typename.columns = ['fundtypecode', 'fundtypename', 'levelnum']
        fundsector['fundtypecode1'] = fundsector['fundtypecode'].str[:8]
        fundsector['fundtypecode2'] = fundsector['fundtypecode'].str[:10]
        fundsector['fundtypecode3'] = fundsector['fundtypecodeorg'].str[:12]
        fundsectoruse = fundsector[fundsector['fundtypecode'].str[:6] == '200101'].reset_index(drop=True).copy()
        typename['fundtypecode1'] = typename['fundtypecode'].str[:8]
        typename['fundtypecode2'] = typename['fundtypecode'].str[:10]
        typename['fundtypecode3'] = typename['fundtypecode'].str[:12]
        typename['fundtypename1'] = typename['fundtypename']
        typename['fundtypename2'] = typename['fundtypename']
        typename['fundtypename3'] = typename['fundtypename']
        typeuse = pd.merge(fundsectoruse[['fundcode', 'fundtypecode1', 'fundtypecode2', 'fundtypecode3',
                                          'entrydateuse']],
                           typename.loc[typename['levelnum'] == 4, ['fundtypecode1', 'fundtypename1']],
                           on=['fundtypecode1'], how='inner')
        typeuse = pd.merge(typeuse,
                           typename.loc[typename['levelnum'] == 5, ['fundtypecode2', 'fundtypename2']],
                           on=['fundtypecode2'], how='left')
        typeuse = pd.merge(typeuse,
                           typename.loc[typename['levelnum'] == 6, ['fundtypecode3', 'fundtypename3']],
                           on=['fundtypecode3'], how='left')

        # typeuse=typeuse.drop_duplicates("fundcode")###############
        duptypedf = typeuse[typeuse.duplicated(['fundcode'], keep=False)]

        if duptypedf.shape[0] > 0:
            raise Exception(date, duptypedf, '基金投资范围板块有重复值！')

        ffuse = pd.merge(finit[['fundcode', 'listdate']], typeuse, on=['fundcode'], how='left')

        # 3. 剔除分级子基金基金
        gradingfund = self.cgfclass.getdata_(date)
        ffuse = ffuse[~ffuse['fundcode'].isin(gradingfund['subfundcode'])].reset_index(drop=True)
        # 这里listdate是基金成立日期，entrydateuse是基金确定类型或转型的日期
        """
        这里需要最近四期的仓位情况
        """
        # ffuse = ffuse[['fundcode', 'fundtypecode', 'listdate', 'entrydateuse']].copy()
        fapdt = self.getfapdt_(date=date)
        if fapdt.shape[0] < ffuse.shape[0]:
            raise Exception(date, 'fapdt shape smaller than ffuse shape!')
        fapdt.fillna(0, inplace=True)
        # fapdt.loc[fapdt['stkpos'] > 100, 'stkpos'] = 100
        fapdt['stkpos'] = fapdt['stkpos'] / 100
        fapdt['hkstkpos'] = fapdt['hkstkpos'] / 100
        fapdt['bondpos'] = fapdt['bondpos'] / 100
        fapdt['covertbondpos'] = fapdt['covertbondpos'] / 100
        fapdt['apos'] = fapdt['stkpos'] - fapdt['hkstkpos']
        fapdt = fapdt.sort_values(['fundcode', 'rptdate'], ascending=[True, False]).reset_index(drop=True)
        fapdt['rptticker'] = 1
        fapdt['rptticker'] = fapdt.groupby(['fundcode'])['rptticker'].cumsum()
        posuse = fapdt[fapdt['rptticker'] <= 4]
        posmat = posuse.pivot_table(index='fundcode', columns='rptticker', values='stkpos')
        posmat.columns = ['pos' + str(int(x) - 1) for x in posmat.columns]
        posmat.reset_index(inplace=True)
        aposmat = posuse.pivot_table(index='fundcode', columns='rptticker', values='apos')
        aposmat.columns = ['apos' + str(int(x) - 1) for x in aposmat.columns]
        aposmat.reset_index(inplace=True)
        bposmat = posuse.pivot_table(index='fundcode', columns='rptticker', values='bondpos')
        bposmat.columns = ['bondpos' + str(int(x) - 1) for x in bposmat.columns]
        bposmat.reset_index(inplace=True)
        cbposmat = posuse.pivot_table(index='fundcode', columns='rptticker', values='covertbondpos')
        cbposmat.columns = ['cbpos' + str(int(x) - 1) for x in cbposmat.columns]
        cbposmat.reset_index(inplace=True)
        fapdt = fapdt.sort_values(['fundcode', 'rptdate'], ascending=[True, False]).reset_index(drop=True)
        fapdt = fapdt[~fapdt.duplicated(['fundcode'], keep='first')].reset_index(drop=True)
        # fapdtuse = fapdt[(fapdt['stkpos'] >= 0) & (fapdt['stkpos'].notnull())].reset_index(drop=True)
        ftemp = ffuse[ffuse['fundcode'].isin(sorted(set(fapdt['fundcode'])))].reset_index(drop=True)
        ftemp = pd.merge(ftemp, fapdt[['fundcode', 'rptdate']], on=['fundcode'], how='inner')
        # ftemp.rename(columns={'apos': 'apos0'}, inplace=True)
        # 加入4期仓位信息
        ftemp = pd.merge(ftemp, posmat, on=['fundcode'], how='inner')
        ftemp = pd.merge(ftemp, aposmat, on=['fundcode'], how='inner')
        ftemp = pd.merge(ftemp, bposmat, on=['fundcode'], how='inner')
        ftemp = pd.merge(ftemp, cbposmat, on=['fundcode'], how='inner')
        ftemp = ftemp.sort_values(['fundcode']).reset_index(drop=True)
        ftemp['listdate'] = ftemp['listdate'].astype(str)
        ftemp['entrydateuse'] = ftemp['entrydateuse'].astype(str)
        # ftemp['listdateuse'] = ftemp[['listdate', 'entrydateuse']].apply(np.nanmax, axis=1)
        # ftemp['listdays'] = ftemp['listdateuse'].apply(
        #     lambda x: (datetime.datetime.strptime(date, '%Y%m%d') - datetime.datetime.strptime(x, '%Y%m%d')).days)
        ftemp['listdateuse'] = ftemp[['listdate', 'entrydateuse']].apply(np.nanmax, axis=1)
        ftemp.loc[ftemp['listdateuse'] == 'nan', 'listdateuse'] = ftemp.loc[ftemp['listdateuse'] == 'nan', 'listdate']
        ftemp['listdays'] = ftemp['listdateuse'].apply(lambda x: (datetime.datetime.strptime(date, '%Y%m%d') -
                                                                  datetime.datetime.strptime(x, '%Y%m%d')).days)

        ftemp.reset_index(drop=True, inplace=True)

        minholding = self.cfundpchredmclass.getdata_(varnames=['minholdingprd'], date=date)
        minholding['minholdingprd'].fillna(0, inplace=True)
        ftemp = pd.merge(ftemp, minholding, on=['fundcode'], how='left')
        ftemp['minholdingprd'].fillna(0, inplace=True)

        # 加入定开基金标签
        fundro = self.cmfsectorclass.getdata_(fundtypecodelist=['2001020e00'], date=date)
        ftemp['ropenticker'] = 0
        ftemp.loc[ftemp['fundcode'].isin(fundro['fundcode']), 'ropenticker'] = 1

        # 加入创新封闭型基金标签
        fundclose = self.cmfsectorclass.getdata_(fundtypecodelist=['2001020600'], date=date)
        ftemp['closefundticker'] = 0
        ftemp.loc[ftemp['fundcode'].isin(fundclose['fundcode']), 'closefundticker'] = 1

        # 加入续存状态开放封闭基金标签
        closefund = self.cmfsectorclass.getdata_(fundtypecodelist=['2001070200'], date=date)
        ftemp['opencloseticker'] = 0
        ftemp.loc[ftemp['fundcode'].isin(closefund['fundcode']), 'opencloseticker'] = 1
        ftemp['fundtypecode1'] = ftemp['fundtypecode1'].astype(str)
        ftemp['fundtypecode2'] = ftemp['fundtypecode2'].astype(str)
        ftemp['fundtypecode3'] = ftemp['fundtypecode3'].astype(str)
        ftemp['fundtypename1'] = ftemp['fundtypename1'].astype(str)
        ftemp['fundtypename2'] = ftemp['fundtypename2'].astype(str)
        ftemp['fundtypename3'] = ftemp['fundtypename3'].astype(str)
        factorfinal = self.solvefac_(ftemp, date)
        return factorfinal


# In[]
# if __name__ == '__main__':
#     self = allfundlist_()
#     # # #     # sigdata = facclass.deletefac_(periods=['20190930', '20191231', '20200331'])
#     # self.initdataclass_()
#     date = '20220201'
#     # sigdata = facclass.getfac_(periods=[dt])
#     sigdata = self.getfac_with_fundcode_(periods=[date])
#
