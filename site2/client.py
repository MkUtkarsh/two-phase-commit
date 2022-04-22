import pymysql
from flask import Flask
from flask import request
from flask import jsonify
import pandas as pd
import requests

app = Flask(__name__)

f = open("log.txt","a+")
coord_url = "http://localhost:5122"

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
    print("Query recieved : ",received_query)

    prepare_ready = True
    try:
        cursor.execute(received_query)
    except:
        prepare_ready = False
    print("prepare status is : ",prepare_ready)

    operation = input("Are you ready to perform above query? Enter yes or no: ").lower()

    if(operation == "yes" and prepare_ready):
        f.write("Ready***\""+received_query+"\"***"+t_id+"\n")
        f.flush()
        return jsonify({'status': "ready "+received_query})
    else:
        f.write("No***\""+received_query+"\"***"+t_id+"\n")
        f.flush()
        return jsonify({'status': "abort "+received_query})

@app.route('/phase2',methods=["POST"])
def run_phase2():
    data = request.get_json()
    received_query = data['query']
    t_id = data['t_id']
    decision = data['decision']

    if(decision == ("Commit")):
        f.write("Commit***\""+received_query+"\"***"+t_id+"\n")
        f.flush()
        connection.commit()
        print("committed at client2 and written in log")
    else:
        f.write("Abort***\""+received_query+"\"***"+t_id+"\n")
        f.flush()
        connection.rollback()
        print("Got abort from coord. Hence aborted the transaction")
    return ('', 204)

def recover():
    df = pd.read_csv("log.txt", sep='\*\*\*',engine='python',header=None)
    df.columns =['status', 'query', 'id']
    print(df)
    print("last row : ",df.iloc[-1])
    last_status = df.iloc[-1]['status']
    last_t_id = df.iloc[-1]['id']
    last_query = df.iloc[-1]['query']
    print('last status : ',last_status)
    if(last_status == 'Ready'):
        curr_url = coord_url+"/get_status"
        data = {
            'query': last_query,
            't_id': last_t_id
        }
        try:
            response = requests.post(curr_url, json=data)
            decision_at_coord = response.json()['decision']
            if(decision_at_coord == 'Commit'):
                last_query = last_query[1:-1]
                cursor.execute(last_query)
                connection.commit()
                f.write("Commit***\""+last_query+"\"***"+last_t_id+"\n")
                f.flush()
                connection.commit()
            else:
                f.write("Abort***\""+last_query+"\"***"+last_t_id+"\n")
                f.flush()
                connection.rollback()
        except:
            print("Phase one failed at : ",curr_url)
            pass

if __name__ == '__main__':
    recover()
    app.run(host='0.0.0.0',port=5125,debug=True)
   