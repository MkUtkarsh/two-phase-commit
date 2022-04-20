import socket
import pymysql.cursors 
import atexit

num_commits,num_of_clients = 0,1
f = open("log.txt","w+")

client_addresses = []
query = ""

connection = pymysql.connect(host='localhost',
                             user='user',
                             password='iiit123',
                             db='coordinator',
                             autocommit=False)
                
cursor = connection.cursor()

def make_connection():

    s = socket.socket()
    port = 8123
    s.bind(("127.0.0.1", port))
    s.listen()

    for i in range(num_of_clients):
        p = s.accept()
        c, _ = p
        client_addresses.append(c)
    return s

# this is phase1
def send_initial_message(query):
    global num_commits
    global client_addresses
    f.write("Prepare "+query+"\n")
    f.flush()

    coord_ready = True
    try:
        cursor.execute(query)
    except:
        coord_ready = False
    print("prepare status is : ",coord_ready)

    if coord_ready == False:
        print("Coordinator is not prepared to run the tran ")
        f.write("Abort "+query+"\n")
        f.flush()
        connection.rollback()
        return coord_ready


    print("Write prepare in log ")
    for client in client_addresses:

        client.send(("Prepare "+query).encode())
        operation_type = client.recv(1024).decode('utf-8').strip()
        if(operation_type == ("ready "+query)):
            num_commits += 1
            print("curr ready count : ",num_commits)
    
    return coord_ready


# This is phase2
def send_final_message():

    global query
    global client_addresses
    global num_of_clients
    global num_commits
    global f

    if(num_commits < num_of_clients):
        f.write("Abort "+query+"\n")
        f.flush()
        connection.rollback()
        print("all clients are not ready hence aborting and writing in log")
        for client in client_addresses:
            client.send(("Abort "+query).encode())
    else:
        f.write("Commit "+query+"\n")
        f.flush()
        connection.commit()
        print("committed at coord and writing in log")
        for client in client_addresses:
            client.send(("Commit "+query).encode())
    
def perform_main_code():
    global query
    global client_addresses
    global num_commits

    
    while True:
        query = input("Enter new query: ")
        # query = "INSERT INTO employee_table VALUES (10,'varun','sde',27);"
        coord_ready = send_initial_message(query)
        if coord_ready:
            send_final_message()
        flag = input("Do you want to fire new query?Type yes or no: ")
        if(flag == "no"):
            for client in client_addresses:
                client.send("End session".encode())
            break
        query = ""
        num_commits = 0


s = make_connection()

def exit_handler():
    f.close()
    s.close()
atexit.register(exit_handler)

perform_main_code()


   
    

