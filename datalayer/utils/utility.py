from time import strftime, localtime, time
import numpy as np
from datetime import date
import math

def normalize_vector(vector: list):
    res = 0
    for i in vector:
        res += i**2
    vector = [ i/(math.sqrt(res)) for i in vector]
    return vector
def format_global_id(number: int):
    s = date.today()
    return s.replace("-",) + "-" +number #yyyy-mm-dd-id
def convert_second_2_datetime(seconds):
    return strftime('%Y-%m-%d %H:%M:%S', localtime(seconds))

def split_datetime(datetime: str):
    d, t = datetime.split(" ") #yyyy-mm-dd
    return d, t  #yyyymmdd

def split_prefix_id(global_id):
    return int(global_id[8:]) #prefix yyyymmdd
