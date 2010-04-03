import geventmysql
import time
import os
import gevent

curtime = time.time if os.name == "posix" else time.clock


C = 50
N = 1000


def task():
    conn = geventmysql.connect(host="127.0.0.1", user="root", password="")
    cur = conn.cursor()
    for i in range(N):
        cur.execute("SELECT 1")
        res = cur.fetchall()
   
    
start = curtime()

gevent.joinall([gevent.spawn(task) for i in range(C)])

elapsed = curtime() - start
num = C * N

print "Performed %d queries in %.2f seconds : %.1f queries/sec" % (num, elapsed, num / elapsed)
