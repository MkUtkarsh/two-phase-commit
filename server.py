import pymysql.cursors 
from flask import Flask
from flask import request
from flask import jsonify
import requests
import uuid

app = Flask(__name__)

num_commits = 0
num_of_clients = 1
f = open("log.txt","w+")

# client_addresses = ["http://localhost:5123","http://localhost:5124","http://localhost:5125"]
client_addresses = ["http://localhost:5124"]
connection = pymysql.connect(host='localhost',
                             user='user',
                             password='iiit123',
                             db='coordinator',
                             autocommit=False)
                
cursor = connection.cursor()


def execute_phase1(query,t_id):
    global num_commits
    print("Write prepare in log ")
    f.write("Prepare \""+query+"\" "+t_id+"\n")
    f.flush()

    coord_ready = True
    try:
        cursor.execute(query)
    except:
        coord_ready = False
    print("prepare status is : ",coord_ready)

    if coord_ready == False:
        print("Coordinator is not prepared to run the tran ")
        f.write("Abort \""+query+"\" "+t_id+"\n")
        f.flush()
        connection.rollback()
        return coord_ready

    for c in client_addresses:
        curr_url = c+"/phase1"
        data = {
            "query": query,
            "t_id": t_id
        }
        response = requests.post(curr_url, json=data)
        return_status = response.json()['status']
        if(return_status == ("ready "+query)):
            num_commits = num_commits+1
            print("curr ready count : ",num_commits)
    return coord_ready

def execute_phase2(query,t_id):
    global num_commits
    if(num_commits < num_of_clients):
        f.write("Abort \""+query+"\" "+t_id+"\n")
        f.flush()
        connection.rollback()
        print("all clients are not ready hence aborting and writing in log")
        for c in client_addresses:
            curr_url = c+"/phase2"
            data = {
                "query": query,
                "t_id": t_id,
                "decision": "Abort"
            }
            response = requests.post(curr_url, json=data)
            # client.send(("Abort "+query).encode())
    else:
        f.write("Commit \""+query+"\" "+t_id+"\n")
        f.flush()
        connection.commit()
        print("committed at coord and writing in log")
        for c in client_addresses:
            curr_url = c+"/phase2"
            data = {
                "query": query,
                "t_id": t_id,
                "decision": "Commit"
            }
            response = requests.post(curr_url, json=data)

    
def main_code():
    global num_commits
    while True:
        num_commits = 0
        query = input("Enter new query: ")
        # query = "INSERT INTO employee_table VALUES (16,'varun','sde',27);"
        t_id = "t"+str(uuid.uuid4().hex)
        coord_ready = execute_phase1(query,t_id) # phase1
        if coord_ready:
            execute_phase2(query,t_id)
        flag = input("Do you want to fire new query?Type yes or no: ")
        if(flag == "no"):
            for client in client_addresses:
                client.send("End session".encode())
            break

if __name__ == '__main__':
#    app.run(host='0.0.0.0',port=5123,debug=True)
   main_code()
