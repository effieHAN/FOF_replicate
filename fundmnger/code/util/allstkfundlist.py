# -*- coding: utf-8 -*-
"""
Created on Sun Mar  8 09:20:48 2020

@author: liukai
"""

from FOF.util.allfundlist import allfundlist_
from FOF.util.factorclass import fundfactor


# In[] 构造每日普通股票以及偏股混合研究池子
class allstkfundlist_(fundfactor):
    def __init__(self, facnameuse='allstkfund', facname='allstkfundlist',
                 varnamelist=('fundtypecode', 'rptdate', 'pos0', 'pos1', 'pos2', 'pos3',
                              'apos0', 'apos1', 'apos2', 'apos3',
                              'ropenticker', 'closefundticker', 'opencloseticker', 'listdays', 'listdate')):
        """
        facnameuse: allstkfund.\n
        ropenticker: 定期开放型基金
        closefundticker：封闭式运作基金
        opencloseticker：续存
        # 普通股票 偏股混合 灵活配置
        '2001010101', '2001010201', '2001010204'\n
        '2001010101', '2001010103','2001010201', '2001010202', '2001010204'\n
        这里计算的listdays是自然日\n
        2001010101	普通股票型基金
        2001010102	被动指数型基金
        2001010103	增强指数型基金
        2001010201	偏股混合型基金
        2001010202	平衡混合型基金
        2001010203	偏债混合型基金
        2001010204	灵活配置型基金
        """
        super().__init__(facname, facnameuse, varnamelist)
        # 可能会用到的基础数据
        self.allfundlistclass = None
        if facnameuse not in ['allstkfund']:
            raise Exception(facnameuse, 'facname must be in allstkfund!')

    def initdataclass_(self):
        self.allfundlistclass = allfundlist_()

    def sigconstructperiods_(self, periods=()):
        # 1. 获取当天存在的全部该类型的基金
        """
        2001010101	普通股票型基金
        2001010102	被动指数型基金
        2001010103	增强指数型基金
        2001010201	偏股混合型基金
        2001010202	平衡混合型基金
        2001010203	偏债混合型基金
        2001010204	灵活配置型基金
        2001010601	股票多空
        2001010604	相对价值
        """
        allfundlist = self.allfundlistclass.getfac_with_fundcode_(periods)
        fundtypecodelist = ['2001010101', '2001010103',
                            '2001010201', '2001010202',
                            '2001010203', '2001010204',
                            '2001010601', '2001010604']
        ftemp = allfundlist[allfundlist['fundtypecode2'].isin(fundtypecodelist)].reset_index(drop=True).copy()
        ftemp['fundtypecode'] = ftemp['fundtypecode2']
        # ftemp=ftemp.drop_duplicates(['date', 'fundcode'])###############
        dupdf = ftemp[ftemp.duplicated(['date', 'fundcode'], keep=False)]
        if dupdf.shape[0] > 0:
            raise Exception(dupdf, '有重复数据！')
        factorfinal = self.solvefacperiods_(ftemp)
        return factorfinal


# In[]
# if __name__ == '__main__':
#     self = allstkfundlist_('allstkfund')
#     # self.initdataclass_()
#     periods = ['20200102']
#     sigdata1 = self.getfac_with_fundcode_(periods=periods)
# # #     sigdata = facclass.getfac_(periods=['20200901'])
# #     facclass.initdataclass_()
# #     fundlist = facclass.sigconstruct_(date='20201009')
