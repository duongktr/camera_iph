from time import strftime, localtime, time
import numpy as np
from datetime import date
import math

def normalize_vector(vector: list):
    v = np.array(vector)
    magnitude = np.linalg.norm(v)
    vector = [v / magnitude for v in vector]
    return vector

def format_global_id(number: int):
    s = date.today()
    return s.replace("-",) + "-" +number #yyyy-mm-dd-id

def convert_second_2_datetime(seconds):
    return strftime('%Y-%m-%d %H:%M:%S', localtime(seconds))

def split_datetime(datetime: str):
    d, t = datetime.split(" ") #yyyy-mm-dd
    return d.replace("-",""), t  #yyyymmdd,

