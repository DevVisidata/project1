from flask import Flask, request, jsonify
from datetime import datetime
import pandas as pd
import calendar
import progressbar
from time import sleep

app = Flask(__name__)

#KONFIG KE DB
import singlestoredb as s2
conn = s2.connect('svc-3479b32e-b09c-4933-a069-d151ce16a097-dml.gcp-jakarta-1.svc.singlestore.com',port = 3306, user='admin', password='Aviata2022*',database='AVIATA_CRP_DASHBOARD',local_infile=True)

def getAllMemberId():
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT idMember, member FROM member')
        memData = cursor.fetchall()
        member_tuple = {value: key for key, value in memData}
        conn.commit()
        return member_tuple
    except:
        return "Error"

#API FLASK
@app.route('/production', methods = ['POST', 'GET'])

def index():
    if request.method == 'POST':
        try:
            api_key = request.headers['api_key']
            if api_key == 'jugldrnjuglfitnf':
                data = request.json
                cur = conn.cursor()
                memberId = getAllMemberId()
                period = data['production_period']
                memberNm = data['production_member'] 
                member_id = memberId[memberNm]
                numRow = 0
                len_data = len(data['production_details'])
                for item in data['production_details']:
                    id_cluster = item.get('id_cluster')
                    cluster = item.get('cluster')
                    id_sub_cluster = item.get('id_sub_cluster')
                    sub_cluster = item.get('sub_cluster')
                    channel = item.get('channel')
                    b_realization = item.get('b_realization')
                    b_domestik = item.get('b_domestik')
                    periode = period
                    id_member = member_id
                    member_name = memberNm
                    dectotal = item.get('dectotal')
                    id_sub_class_component = item.get('id_sub_class_component')
                    sub_class_component = item.get('sub_class_component')
                    measurement_type = item.get('measurement_type')
                    remark = item.get('remark')
                    last_updated = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    data_hasil = [id_cluster,cluster,id_sub_cluster,sub_cluster,channel,b_realization,b_domestik,periode,id_member,member_name,dectotal,id_sub_class_component,sub_class_component,measurement_type,remark,last_updated]    
                    cur.execute("SELECT COUNT(*) FROM productionTest WHERE idCluster = '%s' AND idSubCluster = '%s' AND subCluster = '%s' AND channel = '%s' AND status = '%s' AND flight = '%s' AND period = '%s' AND idMember = '%s'" % (data_hasil[0], data_hasil[2], data_hasil[3], data_hasil[4], data_hasil[5], data_hasil[6], data_hasil[7], data_hasil[8]))
                    count = cur.fetchall()
                    if count[0][0] == 0:
                        stmt = 'INSERT INTO productionTest VALUES(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16)'
                        cur.execute(stmt,data_hasil)
                        numRow+=1
                    elif count[0][0] != 0:
                        break  
                      
                conn.commit()

                if numRow == (len_data):
                    cur = conn.cursor()
                    status = "Success"
                    desc = "Done"
                    print(status, ": ", desc, sep="")
                    file_name = "File JSON" + " " + memberNm
                    period_time = datetime.strptime(period, '%Y-%m-%d')
                    month = calendar.month_name[period_time.month]
                    year = period_time.year
                    period_time = month[:3] + "_" + str(year)
                    list_log = [file_name, "JSON Inject", memberNm, period_time, last_updated, status, desc, numRow]
                    stmt_log = 'INSERT INTO logData VALUES(:1,:2,:3,:4,:5,:6,:7,:8)'
                    cur.execute(stmt_log,list_log)
                    conn.commit()
                    return jsonify(data = list(data), isError= False, message= "Create", statusCode= 201 ), 201
                else:
                    cur = conn.cursor()
                    status = "Error"
                    desc = "Data sudah terdapat di database"
                    print(status, ": ", desc, sep="")
                    file_name = "File JSON" + " " + memberNm
                    period_time = datetime.strptime(period, '%Y-%m-%d')
                    month = calendar.month_name[period_time.month]
                    year = period_time.year
                    period_time = month[:3] + "_" + str(year)
                    list_log = [file_name, "JSON Inject", memberNm, period_time, last_updated, status, desc, 0]
                    stmt_log = 'INSERT INTO logData VALUES(:1,:2,:3,:4,:5,:6,:7,:8)'
                    cur.execute(stmt_log,list_log)
                    conn.commit()
                    return "Data sudah terdapat di database"
        
                
            else:
                cur = conn.cursor()
                status = "Error"
                desc = "Data tidak sesuai"
                print(status, ": ", desc, sep="")
                file_name = "File JSON" + " " + member_name
                period_time = datetime.strptime(period, '%Y-%m-%d')
                month = calendar.month_name[period_time.month]
                year = period_time.year
                period_time = month[:3] + "_" + str(year)
                list_log = [file_name, "JSON Inject", period_time, last_updated, status, desc, 0]
                stmt_log = 'INSERT INTO logData VALUES(:1,:2,:3,:4,:5,:6,:7)'
                cur.execute(stmt_log,list_log)
                conn.commit()
                return "Your key is false. Dont forget enter the key!"
        except:
            return "Dont forget enter the key"

    if request.method == 'GET':
        try:
            api_key = request.headers['api_key']
            if api_key == 'jugldrnjuglfitnf':
                cursor = conn.cursor()
                cursor.execute('SELECT * FROM productionTest')
                data = cursor.fetchall()
                return jsonify(data = list(data),isError= False, message= "Success", statusCode= 200 ), 200
            else:
                return "Your key is false. Dont forget enter the key!"
        except:
            return "Dont forget enter the key"
        

app.run(host='0.0.0.0', port='80')
