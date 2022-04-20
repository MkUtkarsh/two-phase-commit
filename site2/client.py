import socket     

import pymysql      

def perform_operation(query):
    print("Performed "+query+"\n")

s = socket.socket()          
s.connect(('127.0.0.1', 8123))
f = open("log.txt","w+") 


connection = pymysql.connect(host='localhost',
                             user='user',
                             password='iiit123',
                             db='client2',
                             autocommit=False)
cursor = connection.cursor()

while True:
    # putting f here to see output of log while the process is running
    received_msg = s.recv(1024).decode('utf-8').strip()
    query = received_msg[8:]
    print("Query: ",query)

    # phase-1 checking if the client is ready
    prepare_ready = True
    try:
        cursor.execute(query)
    except:
        prepare_ready = False
    print("prepare status is : ",prepare_ready)

    operation = input("Are you ready to perform above query? Enter yes or no: ").lower()

    if(operation == "yes" and prepare_ready):
        f.write("ready "+query+"\n")
        f.flush()
        s.send(("ready "+query).encode())
    else:
        f.write("no "+query+"\n")
        f.flush()
        s.send(("abort "+query).encode())
    
    # Phase 1 has ended


    # waiting for commit or abort from the coordinator
    final_operation = s.recv(1024).decode('utf-8').strip()

    if(final_operation == ("Commit "+query)):
        f.write("Commit "+query+"\n")
        f.flush()
        connection.commit()
        print("committed at client1 and written in log")
    else:
        f.write("Abort "+query+"\n")
        f.flush()
        connection.rollback()
        print("Got abort from coord. Hence aborted the transaction")
   