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
        print(request.form)
        id_cluster = request.form['id_cluster']
        cluster = request.form['cluster']
        id_sub_cluster = request.form['id_sub_cluster']
        sub_cluster = request.form['sub_cluster']
        channel = request.form['channel']
        b_realitation = request.form['b_realitation']
        b_domestik = request.form['b_domestik']    
        periode = request.form['periode']
        id_member = request.form['id_member']
        member_name = request.form['member_name']
        dectotal = request.form['dectotal']
        id_sub_class_component = request.form['id_sub_class_component']
        sub_class_component = request.form['sub_class_component']
        measurement_type = request.form['measurement_type']
        remark = request.form['remark']
        dectotal_by_month = request.form['dectotal_by_month']
        data = [(id_cluster,cluster,id_sub_cluster,sub_cluster,channel,b_realitation,b_domestik,periode,id_member,member_name,dectotal,id_sub_class_component,sub_class_component,measurement_type,remark,dectotal_by_month)]
        stmt = 'INSERT INTO production VALUES(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16)'
        with conn:
            # conn.autocommit(True)
            with conn.cursor() as cur:
                cur.executemany(stmt, data)
        return jsonify(data = list(data),isError= False, message= "Success", statusCode= 200 ), 200

@app.route('/production/<id>', methods = ['POST', 'GET','PUT','DELETE'])
def get_byId(id):
    if request.method == 'GET':
        cursor = conn.cursor()
        cursor.execute(''' SELECT * FROM production WHERE id_cluster=%s ''',(id,))
        data = cursor.fetchone()
        return jsonify(data = data,isError= False, message= "Success", statusCode= 200 ), 200
    if request.method == 'DELETE':
        cursor = conn.cursor()
        data = cursor.execute('''DELETE FROM users WHERE id = (%s)''',(id))
        conn.connection.commit()
        cursor.close()
        return jsonify( message= "Success", statusCode= 200, data =data)
 
app.run(host='0.0.0.0')
