from flask import Flask, request, jsonify

app = Flask(__name__)

#KONFIG KE DB
import singlestoredb as s2
conn = s2.connect('svc-3479b32e-b09c-4933-a069-d151ce16a097-dml.gcp-jakarta-1.svc.singlestore.com',port = 3306, user='admin', password='Aviata2022*',database='AVIATA_CRP_DASHBOARD',local_infile=True)

#API FLASK
@app.route('/production', methods = ['POST', 'GET'])
def index():
    
    if request.method == 'GET':
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM production')
        data = cursor.fetchall()
        return jsonify(data = list(data),isError= False, message= "Success", statusCode= 200 ), 200

        # for row in cursor.fetchall():
        #     print(row)
        # with conn:
        #     # conn.autocommit(True)
        #     with conn.cursor() as cur:
        #         cur.execute('SELECT * FROM production')
        #         for row in cur.fetchall():
        #             print(row)
    
    if request.method == 'POST':
        print(request.json)
        id_cluster = request.json['id_cluster']
        cluster = request.json['cluster']
        id_sub_cluster = request.json['id_sub_cluster']
        sub_cluster = request.json['sub_cluster']
        channel = request.json['channel']
        b_realitation = request.json['b_realitation']
        b_domestik = request.json['b_domestik']    
        periode = request.json['periode']
        id_member = request.json['id_member']
        member_name = request.json['member_name']
        dectotal = request.json['dectotal']
        id_sub_class_component = request.json['id_sub_class_component']
        sub_class_component = request.json['sub_class_component']
        measurement_type = request.json['measurement_type']
        remark = request.json['remark']
        data = [(id_cluster,cluster,id_sub_cluster,sub_cluster,channel,b_realitation,b_domestik,periode,id_member,member_name,dectotal,id_sub_class_component,sub_class_component,measurement_type,remark)]
        stmt = 'INSERT INTO production VALUES(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15)'
        with conn:
            # conn.autocommit(True)
            with conn.cursor() as cur:
                cur.executemany(stmt, data)
        return jsonify(data = list(data),isError= False, message= "Success", statusCode= 200 ), 200

@app.route('/production/')
def prodcution():
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM production')
    data = cursor.fetchall()
    return jsonify(data = list(data),isError= False, message= "Success", statusCode= 200 ), 200

 
app.run(host='0.0.0.0')
