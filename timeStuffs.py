import calendar, time
import pandas as pd

# Human to UNIX
def toUnix(timeStamp, timeFmt):
    t = time.strptime(timeStamp, timeFmt)
    unix = calendar.timegm(time.struct_time(t))
    print(unix)
    return unix

def toHuman(unixTime):
    ret = pd.to_datetime(unixTime, unit='s')
    print(ret)

if __name__ == '__main__':
    a = toUnix('2018/01/01', '%Y/%m/%d')
    b = toHuman(a)