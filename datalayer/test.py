import time
import datetime
import random

ti = time.time()

datetime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ti))

print(datetime)
# d, t = datetime.split(" ")

# d=  d.replace("-","")

# a = 5
# print(d+str(a))