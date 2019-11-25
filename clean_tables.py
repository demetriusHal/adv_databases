import psycopg2 


def getConn():
    conn = psycopg2.connect(
    host="localhost",
    database="mydb",
    user="postgres",
    password="yolo1993",

    )
    return conn


conn = getConn()
cursor = conn.cursor()


cursor.execute('delete from blocks returning *;')
cursor.execute('delete from access returning *;')
cursor.execute('delete from log returning *;')

conn.commit()
cursor.close()
conn.close()   