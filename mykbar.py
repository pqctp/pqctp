#coding:utf-8

import sqlite3

import csv

from time import time

conn = sqlite3.connect('mfutures.db3', check_same_thread = False)


cmd = "DROP TABLE IF EXISTS " + "kbar"
conn.execute(cmd)

cmd = "CREATE TABLE IF NOT EXISTS " + "kbar" \
          + " (id INTEGER PRIMARY KEY NULL, inst TEXT NULL, open DOUBLE NULL, high DOUBLE NULL, low DOUBLE NULL, close DOUBLE NULL, volume INTEGER NULL, TradingDay TEXT NULL, time TEXT NULL)"

conn.execute(cmd)

start = time()
reader = csv.DictReader(file("rb0000_1min.csv", 'r'))

for d in reader:

    dinst = 'rb000'
    dopen = float(d['Open'])
    dhigh = float(d['High'])
    dlow = float(d['Low'])
    dclose = float(d['Close'])
    ddate = d['Date']
    dtime = d['Time']
    dvolume = int(d['TotalVolume'])

    #print dopen, dvolume


    conn.execute("INSERT INTO %s (inst, open, high, low, close, volume, TradingDay, time) VALUES ('%s', %f, %f, %f, %f, %d, '%s','%s')" \
                 % ('kbar', dinst, dopen, dhigh, dlow, dclose, dvolume, ddate, dtime ))


conn.commit()

print u'插入完毕，耗时：%s' % (time()-start)


conn.commit()

conn.close()


#
# if __name__=="__main__":
#     # 'http://stock2.finance.sina.com.cn/futures/api/json.php/IndexService.getInnerFuturesDailyKLine?symbol=M1701'
#     base_url = 'http://stock2.finance.sina.com.cn/futures/api/json.php/IndexService.getInnerFuturesDailyKLine?symbol='
#     for symbol in inst_strategy.keys():
#         url = base_url + symbol
#         print 'url = ' + url
#         results = json.load(urllib.urlopen(url))
#
#         for r in results:
#             # r -- ["2016-09-05","2896.000","2916.000","2861.000","2870.000","1677366"]  open, high, low, close
#             conn.execute(
#                 "INSERT INTO %s (inst, open, high, low, close, volume, TradingDay,time) VALUES ('%s', %f, %f, %f, %f, %d, '%s','%s')"
#                 % (symbol + suffix_list[0], symbol, float(r[1]), float(r[2]), float(r[3]), float(r[4]), int(r[5]), r[0], '15:00:00'))
#             conn.commit()
