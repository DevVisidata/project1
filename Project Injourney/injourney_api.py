from flask import Flask, request, jsonify

app = Flask(__name__)

#KONFIG KE DB
import singlestoredb as s2
conn = s2.connect('svc-3479b32e-b09c-4933-a069-d151ce16a097-dml.gcp-jakarta-1.svc.singlestore.com',port = 3306, user='admin', password='Aviata2022*',database='AVIATA_CRP_DASHBOARD',local_infile=True)

def getAllMemberId():
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT Idmember, memberName FROM member')
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
                for item in data['production_details']:
                    id_cluster = item.get('id_cluster')
                    cluster = item.get('cluster')
                    id_sub_cluster = item.get('id_sub_cluster')
                    sub_cluster = item.get('sub_cluster')
                    channel = item.get('channel')
                    b_realitation = item.get('b_realitation')
                    b_domestik = item.get('b_domestik')
                    periode = period
                    id_member = member_id
                    member_name = memberNm
                    dectotal = item.get('dectotal')
                    id_sub_class_component = item.get('id_sub_class_component')
                    sub_class_component = item.get('sub_class_component')
                    measurement_type = item.get('measurement_type')
                    remark = item.get('remark')
                    data_hasil = [id_cluster,cluster,id_sub_cluster,sub_cluster,channel,b_realitation,b_domestik,periode,id_member,member_name,dectotal,id_sub_class_component,sub_class_component,measurement_type,remark]
                    stmt = 'INSERT INTO productionTest VALUES(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15)'
                    cur.execute(stmt,data_hasil)

                conn.commit()
                return jsonify(data = list(data), isError= False, message= "Create", statusCode= 201 ), 201
                
            else:
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
        

app.run(host='0.0.0.0')

# , port='80'
