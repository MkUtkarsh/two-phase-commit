import pymysql
from flask import Flask
from flask import request
from flask import jsonify
import requests

app = Flask(__name__)

f = open("log.txt","w+")


connection = pymysql.connect(host='localhost',
                             user='user',
                             password='iiit123',
                             db='client2',
                             autocommit=False)
cursor = connection.cursor()

@app.route('/phase1',methods=["POST"])
def run_phase1():
    data = request.get_json()
    received_query = data['query']
    t_id = data['t_id']

    prepare_ready = True
    try:
        cursor.execute(received_query)
    except:
        prepare_ready = False
    print("prepare status is : ",prepare_ready)

    operation = input("Are you ready to perform above query? Enter yes or no: ").lower()

    if(operation == "yes" and prepare_ready):
        f.write("Ready \""+received_query+"\" "+t_id+"\n")
        f.flush()
        return jsonify({'status': "ready "+received_query})
    else:
        f.write("No \""+received_query+"\" "+t_id+"\n")
        f.flush()
        return jsonify({'status': "abort "+received_query})

@app.route('/phase2',methods=["POST"])
def run_phase2():
    data = request.get_json()
    received_query = data['query']
    t_id = data['t_id']
    decision = data['decision']

    if(decision == ("Commit")):
        f.write("Commit \""+received_query+"\" "+t_id+"\n")
        f.flush()
        connection.commit()
        print("committed at client1 and written in log")
    else:
        f.write("Abort \""+received_query+"\" "+t_id+"\n")
        f.flush()
        connection.rollback()
        print("Got abort from coord. Hence aborted the transaction")
    return ('', 204)
    

if __name__ == '__main__':
   app.run(host='0.0.0.0',port=5125,debug=True)
   