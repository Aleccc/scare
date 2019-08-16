# -*- coding: utf-8 -*-
"""
Created on Fri May 17 17:46:09 2019

@author: GAS01409
"""

import time


def timer(func):
    def wrapper(*args, **kwargs):
        tic = time.time()
        x = func(*args, **kwargs)
        toc = time.time()
        name = func.__name__
        print('{} time: {:.2f}sec'.format(name, toc - tic))
        return x

    return wrapper
