import pymysql.cursors 
from flask import Flask
from flask import request
from flask import jsonify
import requests
import uuid
import threading
import time
import pandas as pd

# log = logging.getLogger('werkzeug')
# log.setLevel(logging.ERROR)

app = Flask(__name__)

num_commits = 0
num_of_clients = 2
f = open("log.txt","a+")

# client_addresses = ["http://localhost:5123","http://localhost:5124","http://localhost:5125"]
client_addresses = ["http://localhost:5124","http://localhost:5125"]
connection = pymysql.connect(host='localhost',
                             user='user',
                             password='iiit123',
                             db='coordinator',
                             autocommit=False)
                
cursor = connection.cursor()

def thread_function(url,query,t_id):
    global num_commits
    curr_url = url+"/phase1"
    data = {
        "query": query,
        "t_id": t_id
    }
    try:
        response = requests.post(curr_url, json=data)
        return_status = response.json()['status']
        if(return_status == ("ready "+query)):
            num_commits = num_commits+1
            print("curr ready count : ",num_commits)
    except:
        print("Phase one failed at : ",url)



def execute_phase1(query,t_id):
    global num_commits
    print("Write prepare in log ")
    f.write("Prepare***\""+query+"\"***"+t_id+"\n")
    f.flush()

    coord_ready = True
    try:
        cursor.execute(query)
    except:
        coord_ready = False
    print("prepare status is : ",coord_ready)

    if coord_ready == False:
        print("Coordinator is not prepared to run the tran ")
        f.write("Abort***\""+query+"\"***"+t_id+"\n")
        f.flush()
        connection.rollback()
        return coord_ready

    threads = []
    for c in client_addresses:
        x = threading.Thread(target=thread_function, args=(c,query,t_id))
        threads.append(x)
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return coord_ready

def execute_phase2(query,t_id):
    global num_commits
    while True:
        continue_phase_2 = input("Shall I continue to phase 2 (yes or no): ")
        if(continue_phase_2 == "yes"):
            break
    if(num_commits < num_of_clients):
        f.write("Abort***\""+query+"\"***"+t_id+"\n")
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
            try:
                response = requests.post(curr_url, json=data)
            except:
                print("Couldn't send decision to : ",c)
            # client.send(("Abort "+query).encode())
    else:
        f.write("Commit***\""+query+"\"***"+t_id+"\n")
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
            try:
                response = requests.post(curr_url, json=data)
            except:
                print("Couldn't send decision to : ",c)

    
def main_code():
    global num_commits
    time.sleep(1)
    while True:
        num_commits = 0
        query = input("Enter new query: ")
        # query = "INSERT INTO employee_table VALUES (108,'varun','sde',27);"
        t_id = "t"+str(uuid.uuid4().hex)
        coord_ready = execute_phase1(query,t_id) # phase1
        if coord_ready:
            execute_phase2(query,t_id)
        flag = input("Do you want to fire new query?Type yes or no: ")
        if(flag == "no"):
            break

# gets the transaction id and returns if it was commited or aborted
@app.route('/get_status',methods=["POST"])
def get_status():
    data = request.get_json()
    received_query = data['query']
    t_id = data['t_id']
    df = pd.read_csv("log.txt", sep='\*\*\*',engine='python',header=None)
    df.columns =['status', 'query', 'id']
    # print("database from coord")
    # print(df)
    status_list = df.loc[df['id'] == t_id]['status'].to_list()
    # print("status list is : ",status_list)
    #status_list will contain prepare + (either Commit or Abort)
    if 'Commit' in status_list:
        # print("entered commit return part")
        return jsonify({'decision': "Commit"})
    else:
        # print("entered abort return part")
        return jsonify({'decision': "Abort"})

if __name__ == '__main__':
    x = threading.Thread(target=main_code, args=())
    x.start()
    app.run(host='0.0.0.0',port=5122,debug=False)