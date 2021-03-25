from flask import Flask, Response, render_template, request
from flask_cors import CORS
from bson.json_util import dumps
from sueldos import connection
import urllib3

app = Flask(__name__, static_url_path='')
cors = CORS(app, resources={r"/api/*": {"origins":"*"}})

@app.route("/")
def index():
    return app.send_static_file("index.html")

@app.route("/api/nomina",methods=['GET', 'POST'])
def api_root():
    db=connection()
    pageNumber = int(request.args.get('page'))
    nPerPage = int(request.args.get('rows'))
    response=dumps(db.nomina.find().skip(((pageNumber - 1 ) * nPerPage ) if (pageNumber > 0) else 0).limit(nPerPage))
    return Response(response=response, status=200, mimetype='application/json')

@app.route("/api/<path:q>",methods=['GET', 'POST'])
def query(q):
    db=connection()
    response=dumps(db.nomina.find({
        "$or":[
                {"descripcionEntidad": {'$regex': str(urllib3.util.parse_url(q)), "$( ( pageNumber - 1 ) * nPerPage ) : 0options" :'i'}}, 
                {"codigoPersona": {'$regex': q}}, 
                {"nombres": {'$regex': str(urllib3.util.parse_url(q)), "$options" :'i'}},
                {"apellidos": {'$regex': str(urllib3.util.parse_url(q)), "$options" :'i'}}
            ]
        })
    )
    return Response(response=response, status=200, mimetype='application/json')


if __name__=="__main__":
        app.run()