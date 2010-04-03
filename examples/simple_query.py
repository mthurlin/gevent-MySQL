import geventmysql

conn = geventmysql.connect(host="127.0.0.1", user="root", password="")

cursor = conn.cursor()
cursor.execute("SELECT 1")

print cursor.fetchall()

cursor.close()
conn.close()

        