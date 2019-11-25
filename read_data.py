import pandas as pd

import run_sql

#WARNING
#This insertion system needs to run in specific steps
#to account for dependecies
# first id that exists


conn = run_sql.getConn()
cursor = conn.cursor()


def getFirstId():
    cursor.execute('''select nextval(pg_get_serial_sequence('log', 'id')) as new_id;''')
    return cursor.fetchall()[0][0]

startId = getFirstId()


def reformatDate(ts):
    dl = list(ts[0])
    hl = list(ts[1])
    return '20{}{}-{}{}-{}{} {}{}:{}{}:{}{}'.format(
        dl[4],
        dl[5],
        dl[2],
        dl[3],
        dl[0],
        dl[1],
        hl[0],
        hl[1],
        hl[2],
        hl[3],
        hl[4],
        hl[5],
    )



def nSFileRead():
    
    
    


    filepath='logs/HDFS_FS_Namesystem.log'
    sourceIp = 7
    typeField = 9
    blocksStart = 10
    blockPrefix = 'blk_'
    destIpOffset = 2

    def getAllBlocks(line):
        i = 10
        blist = []
        while  i < len(line) and line[i].startswith(blockPrefix):
            #note to self, check if the dash needs to be added
            blist.append(line[i][4:])
            i += 1
        
        return (blist, i)

    def insertEntry(date,source,logtype,blklist,dests):
        global startId
        startId = startId + 1
        #first table, 
        query = ''' insert into log (time, source_ip, type)
        values ('{}', '{}', '{}')'''.format(date, source, logtype)
        cursor.execute(query)
        #second table missing quote on PURPOSE
        protoQuery = '''insert into blocks (log_id, dest_ip, block_requested, size)
                values ({},{},'{}',NULL) '''
        

        if dests == []:
            dests = ['NULL']
        else:
            #hacky way to add quotes
            dests = ["'"+dest+"'" for dest in dests]
        for block in blklist:
            for dest in dests:
                query = protoQuery.format(
                    startId,
                    dest,
                    block,
                )
                cursor.execute(query)






    cnt = 0
    with open(filepath) as fp:
        line = fp.readline()
        while line:
            cnt += 1
            line = line.split()
            #get date
            date = reformatDate(line[0:2])
            #get sourceIp
            source = line[sourceIp]
            #get type
            logtype = line[typeField]
            #get all blocks
            (blklist, ofst) = getAllBlocks(line)
            #get dest ip
            ofst += 2
            dests = []
            while ofst < len(line):
                dests.append(line[ofst])
                ofst += 1
             

            insertEntry(date,source,logtype,blklist,dests)
            #read next
            if (cnt > 1000):
                break
            line = fp.readline()

    #commit
    conn.commit()




def DXFileRead():


    filepath='logs/HDFS_DataXceiver.log'


    sourceIp = 9
    typeField = 5
    blocksStart = 7
    blockPrefix = 'blk_'
    destIpOffset = 11
    sizeOffset = 14


    def insertEntry(date,source, logtype, block, dest, size):
        global startId
        startId = startId + 1
        #first table, 
        query = ''' insert into log (time, source_ip, type)
        values ('{}', '{}', '{}')'''.format(date, source, logtype)
        cursor.execute(query)
        #second table missing quote on PURPOSE

        if size != 'NULL':
            size = "'"+size+"'" 

        query = '''insert into blocks (log_id, dest_ip, block_requested, size)
                values ({},'{}','{}',{}) '''.format(
                    startId,
                    dest,
                    block,
                    size
                )

        cursor.execute(query)
           
    cnt = 0
    with open(filepath) as fp:
        line = fp.readline()



        while line:
            
            cnt += 1
            line = line.split()


            #hacky fix to check for Served case
            if line[6] == 'Served':
                sourceIp = 5
                typefield = 6
                blocksStart = 8
                destIpOffset = 10
                sizeOffset = 13
            elif line[5] == 'Receiving' or line[5] == 'Received':
                sourceIp = 9
                typeField = 5
                blocksStart = 7
                destIpOffset = 11
                sizeOffset = 14
            else:
                line = fp.readline()
                continue
            
            #get date
            date = reformatDate(line[0:2])
            #get sourceIp
            source = line[sourceIp]
            #get type
            logtype = line[typeField]
            #get block
            block = line[blocksStart][4:]
            #get dest
            dest = line[destIpOffset]
            #get size
            size = 'NULL'
            if (sizeOffset < len(line)):
                size = line[sizeOffset]
            
            insertEntry(date,source, logtype, block, dest, size)
            

            #read next
            line = fp.readline()
            if (cnt > 1000):
                break

    #commit
    conn.commit()




def accessFileRead():
    #this has been complete nightmare...
    global startId

    def processRequest(req):
        req = req.split(' ')
        return (req[0], req[1])

    def processDate(date):

        monthToNum = {
            'Jan' : 1,
            'Feb' : 2,
            'Mar' : 3,
            'Apr' : 4,
            'May' : 5,
            'Jun' : 6,
            'Jul' : 7,
            'Aug' : 8,
            'Sep' : 9, 
            'Oct' : 10,
            'Nov' : 11,
            'Dec' : 12
        }

        date = date[1:]
        day = date.split('/')
        time = day[2].split(':')
        day[2] = time[0]
        time = time[1:]
        return '{}-{}-{} {}:{}:{}'.format(
            day[2],
            monthToNum[day[1]],
            day[0],
            time[0],
            time[1],
            time[2]
        )



    import csv
    df = pd.read_csv('logs/access_small.log', sep=' ', quotechar='"',  engine='python', error_bad_lines=False)
    dates = [processDate(df.date[i]) for i in range(df.shape[0])]
    reqRes = [processRequest(df.cmd[i]) for i in range(df.shape[0])]

    for i in range(df.shape[0]):
        startId = startId + 1
        query = ''' insert into log (time, source_ip, type)
        values ('{}', '{}', 'Access')'''.format(dates[i], df.sourceIp[i])
        print query
        cursor.execute(query)

        user = 'NULL' if (df.user[i] == '-') else "'"+ df.user[i]+"'"
        print df.referer[i]
        referer = 'NULL' if (df.referer[i] == '-') else "'"+ df.referer[i]+"'"
        resSize = 'NULL' if (df.resSize[i] == '-') else "'"+ df.resSize[i]+"'"
        query = ''' insert into access (user_id, http_method, resource, response, response_size, referer, user_string, log_id)
        values ({}, '{}', '{}','{}',{},{},'{}',{})
        '''.format(
            user,
            reqRes[i][0],
            reqRes[i][1],
            df.resp[i],
            resSize,
            referer,
            df.agent[i],
            startId
        )
        print query
        cursor.execute(query)
    conn.commit()






#nSFileRead()
#DXFileRead()

accessFileRead()


#clean up
cursor.close()
conn.close() 