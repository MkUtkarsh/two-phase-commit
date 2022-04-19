import socket     

import pymysql.cursors          

def perform_operation(query):
    print("Performed "+query+"\n")

f = open("log.txt","w")
s = socket.socket()          
s.connect(('127.0.0.1', 8000))


connection = pymysql.connect(host='localhost',
                             user='user',
                             password='iiit123',
                             db='client1',
                             autocommit=False)
cursor = connection.cursor()

def test():
    query = "INSERT INTO employee_table VALUES (6,'varun','sde',27);"
    cursor.execute(query)
    # connection.rollback()
    connection.commit()
    # connection.close()
    return


while True:

    received_msg = s.recv(1024).decode('utf-8').strip()
    print(received_msg)
    query = received_msg[9:-1]
    # print("query is :",query)
    # while(received_msg == ""):
    #     pass
    
    if(received_msg == "End session"):
        break

    # _,q = received_msg.split("(")
    # query = q[:-1]
    print("Query: ",query)

    operation = input("Are you ready to perform above query? Enter yes or no: ").lower()

    if(operation == "yes"):
        f.write("ready ("+query+")\n")
        s.send(("ready "+query).encode())
    else:
        f.write("no ("+query+")\n")
        s.send(("abort "+query).encode())

    final_operation = s.recv(1024).decode('utf-8').strip()

    while(final_operation == ""):
        pass

    if(final_operation == ("Commit "+query)):
        f.write("Commit ("+query+")\n")
        perform_operation(query)
    else:
        f.write("Abort ("+query+")\n")

f.close()
s.close()
test()
   