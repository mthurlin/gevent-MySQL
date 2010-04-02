import geventmysql
import time
import os
import gevent

curtime = time.time if os.name == "posix" else time.clock


C = 50
N = 1000


def task():
    conn = geventmysql.connect(host="127.0.0.1", user="root", passwd="")
    cur = conn.cursor()
    for i in range(N):
        cur.execute("SELECT 1")
        res = cur.fetchall()
   
    
start = curtime()
t = []
for i in range(C):
    t.append(gevent.spawn(task))
    
gevent.joinall(t)

elapsed = curtime() - start
num = C * N

print "Performed %d queries in %.2f seconds : %.1f queries/sec" % (num, elapsed, num / elapsed)
