import socket
num_commits,num_of_clients = 0,3
f = open("log.txt","w")

client_addresses = []
query = ""

def make_connection():

    s = socket.socket()
    port = 8000
    s.bind(("127.0.0.1", port))
    s.listen()

    for i in range(num_of_clients):
        p = s.accept()
        c, _ = p
        client_addresses.append(c)
    return s

def send_initial_message(query):
    global num_commits
    global client_addresses

    for client in client_addresses:
        client.send(("Prepare ("+query+")").encode()) 
        operation_type = client.recv(1024).decode('utf-8').strip()

        while(operation_type == ""):
            pass

        if(operation_type == ("ready "+query)):
            num_commits += 1

def send_final_message():

    global query
    global client_addresses
    global num_of_clients
    global num_commits
    global f

    if(num_commits < num_of_clients):
        f.write("Abort ("+query+")\n")
        for client in client_addresses:
            client.send(("Abort "+query).encode())
    else:
        f.write("Commit ("+query+")\n")
        for client in client_addresses:
            client.send(("Commit "+query).encode())
    
def perform_main_code():
    global query
    global client_addresses
    global num_commits

    
    while True:
        query = input("Enter new query: ")
        send_initial_message(query)
        send_final_message()
        flag = input("Do you want to fire new query?Type yes or no: ")
        if(flag == "no"):
            for client in client_addresses:
                client.send("End session".encode())
            break
        query = ""
        num_commits = 0

s = make_connection()
perform_main_code()

f.close()
s.close()
   
    

