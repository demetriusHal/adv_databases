import psycopg2 



def runQ(conn):

    cur = conn.cursor()

    cur.execute("""select * from my_user
    """)

    for row in cur:
        print(row)


    conn.close()


def getConn():
    conn = psycopg2.connect(
    host="localhost",
    database="mydb",
    user="postgres",
    password="yolo1993",

    )
    return conn
