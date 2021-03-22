from flask import Flask, Response, render_template
from flask_cors import CORS
from bson.json_util import dumps
from requests.api import request
from sueldos import connection
import urllib3

app = Flask(__name__, static_url_path='')
cors = CORS(app, resources={r"/api/*": {"origins":"*"}})

@app.route("/")
def index():
    return app.send_static_file("index.html")

@app.route("/api/nomina/<path:q>",methods=['GET', 'POST'])
def api_root(q):
    db=connection()
    response=dumps(db.nomina.find({'anio':int(q)}).limit(5))
    return Response(response=response, status=200, mimetype='application/json')

@app.route("/api/<path:q>",methods=['GET', 'POST'])
def query(q):
    db=connection()
    response=dumps(db.nomina.find({
        "$or":[
                {"descripcionEntidad": {'$regex': str(urllib3.util.parse_url(q)), "$options" :'i'}}, 
                {"codigoPersona": {'$regex': q}}, 
                {"nombres": {'$regex': str(urllib3.util.parse_url(q)), "$options" :'i'}},
                {"apellidos": {'$regex': str(urllib3.util.parse_url(q)), "$options" :'i'}}
            ]
        })
    )
    return Response(response=response, status=200, mimetype='application/json')


if __name__=="__main__":
        app.run()