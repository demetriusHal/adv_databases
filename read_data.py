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
            line = fp.readline()
            if (cnt == 790):
                break
    #commit
    conn.commit()

    

    
           



nSFileRead()


#clean up
cursor.close()
conn.close() 