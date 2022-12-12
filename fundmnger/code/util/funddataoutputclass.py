# -*- coding: utf-8 -*-



import numpy as np
import pandas as pd
from database.code.windsql.cmfdesc import cmfdesc_
from FOF.util.getfundcode import getfundcodeclass_

# raise Exception()

# In[]
def singleton(cls):
    _instances = {}

    def get_instance(*args, **kwargs):
        if cls not in _instances:
            _instances[cls] = cls(*args, **kwargs)
        return _instances[cls]

    return get_instance


@singleton
class funddataoutput_:

    def __init__(self):
        self.desclass = cmfdesc_()
        self.getfundcodeclass = getfundcodeclass_()
        self.compinfo = None



    def getcominfo_(self, maxdate = None,fundcodelist=None):
        if maxdate is not None:
            compinfo=self.desclass.getdata_(varnames=['compname'],todt=maxdate,)
            compinfo = self.getfundcodeclass.getfac_(compinfo, date=maxdate)
        else:
            raise Exception('params are illegal!')
        compinfo=compinfo[compinfo["fundcode"].isin(fundcodelist)]
        self.compinfo=compinfo
        return compinfo

#
if __name__ == '__main__':
    self = funddataoutput_()
    aa = self.getcominfo_(maxdate='20210101',fundcodelist=['000001.OF','000021.OF'])
