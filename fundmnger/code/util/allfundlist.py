# -*- coding: utf-8 -*-
"""
Created on Sun Mar  8 09:20:48 2020

@author: liukai
"""

from FOF.util.fundlistutilclass import fundlistutilclass_


# In[] 构造初始基金池
def singleton(cls):
    _instances = {}

    def get_instance(*args, **kwargs):
        if cls not in _instances:
            _instances[cls] = cls(*args, **kwargs)
        return _instances[cls]

    return get_instance


@singleton
class allfundlist_(fundlistutilclass_):
    def __init__(self):
        """
        facnameuse: allstkfund.\n
        ropenticker: 定期开放型基金
        closefundticker：封闭式运作基金
        opencloseticker：续存
        """
        super().__init__(facnameuse='allfund')
        # 可能会用到的基础数据
        self.cmfdescclass = None
        self.cgfclass = None
        self.cmfsectorclass = None
        self.fap = None
        self.cfundpchredmclass = None
        self.typenameclass = None

    def getfapdt_(self, date=None):
        fapdt = self.fap.getdata_(varnames=['hkstkval', 'stkpos', 'hkstkpos', 'bondpos', 'covertbondpos'], todt=date)
        fapdt = fapdt[fapdt['anndate'] < date].reset_index(drop=True)
        return fapdt



# In[]
# if __name__ == '__main__':
#     self = allfundlist_()
#     # # #     # sigdata = facclass.deletefac_(periods=['20190930', '20191231', '20200331'])
#     # self.initdataclass_()
#     date = '20220201'
#     # sigdata = facclass.getfac_(periods=[dt])
#     sigdata = self.getfac_with_fundcode_(periods=[date])
#
