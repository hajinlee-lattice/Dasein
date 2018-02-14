import pandas as pd


PD_VERSION = pd.__version__

def pd_before_17():
    return int(PD_VERSION.split(".")[1]) < 17

def pd_before_14():
    return int(PD_VERSION.split(".")[1]) < 14
