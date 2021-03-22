import asyncio
import json
import aiohttp
import urllib3
import math
import pymongo
import datetime

from pymongo.errors import BulkWriteError

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def connection():
    cliente=pymongo.MongoClient("mongodb://localhost:27017/")
    db=cliente["nominapy"]
    return db

async def nomina(year, month, start, length):
    headers={
        "Content-Type": "application/json"
    }
    async with aiohttp.ClientSession() as session:
        async with session.post('https://datos.hacienda.gov.py/odmh-core/rest/nomina/datatables?', data="draw=6&columns%5B0%5D%5Bdata%5D=anio&columns%5B0%5D%5Bname%5D=anio&columns%5B0%5D%5Bsearchable%5D=true&columns%5B0%5D%5Borderable%5D=true&columns%5B0%5D%5Bsearch%5D%5Bvalue%5D="+str(year)+"&columns%5B0%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B0%5D%5Btype%5D=string&columns%5B0%5D%5BignoreFilter%5D=false&columns%5B0%5D%5Bdefault%5D=&columns%5B1%5D%5Bdata%5D=mes&columns%5B1%5D%5Bname%5D=mes&columns%5B1%5D%5Bsearchable%5D=true&columns%5B1%5D%5Borderable%5D=true&columns%5B1%5D%5Bsearch%5D%5Bvalue%5D="+str(month)+"&columns%5B1%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B1%5D%5Btype%5D=string&columns%5B1%5D%5BignoreFilter%5D=false&columns%5B1%5D%5Bdefault%5D=&columns%5B2%5D%5Bdata%5D=descripcionNivel&columns%5B2%5D%5Bname%5D=&columns%5B2%5D%5Bsearchable%5D=true&columns%5B2%5D%5Borderable%5D=true&columns%5B2%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B2%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B2%5D%5Btype%5D=combo&columns%5B2%5D%5BignoreFilter%5D=false&columns%5B2%5D%5Bdefault%5D=&columns%5B3%5D%5Bdata%5D=descripcionEntidad&columns%5B3%5D%5Bname%5D=&columns%5B3%5D%5Bsearchable%5D=true&columns%5B3%5D%5Borderable%5D=true&columns%5B3%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B3%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B3%5D%5Btype%5D=combo&columns%5B3%5D%5BignoreFilter%5D=false&columns%5B3%5D%5Bdefault%5D=&columns%5B4%5D%5Bdata%5D=codigoPersona&columns%5B4%5D%5Bname%5D=cedula&columns%5B4%5D%5Bsearchable%5D=true&columns%5B4%5D%5Borderable%5D=true&columns%5B4%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B4%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B4%5D%5Btype%5D=string&columns%5B4%5D%5BignoreFilter%5D=false&columns%5B4%5D%5Bdefault%5D=&columns%5B5%5D%5Bdata%5D=nombres&columns%5B5%5D%5Bname%5D=&columns%5B5%5D%5Bsearchable%5D=true&columns%5B5%5D%5Borderable%5D=true&columns%5B5%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B5%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B5%5D%5Btype%5D=combo&columns%5B5%5D%5BignoreFilter%5D=false&columns%5B5%5D%5Bdefault%5D=&columns%5B6%5D%5Bdata%5D=apellidos&columns%5B6%5D%5Bname%5D=&columns%5B6%5D%5Bsearchable%5D=true&columns%5B6%5D%5Borderable%5D=true&columns%5B6%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B6%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B6%5D%5Btype%5D=combo&columns%5B6%5D%5BignoreFilter%5D=false&columns%5B6%5D%5Bdefault%5D=&columns%5B7%5D%5Bdata%5D=sexo&columns%5B7%5D%5Bname%5D=&columns%5B7%5D%5Bsearchable%5D=true&columns%5B7%5D%5Borderable%5D=true&columns%5B7%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B7%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B7%5D%5Btype%5D=string&columns%5B7%5D%5BignoreFilter%5D=false&columns%5B7%5D%5Bdefault%5D=&columns%5B8%5D%5Bdata%5D=discapacidad&columns%5B8%5D%5Bname%5D=&columns%5B8%5D%5Bsearchable%5D=true&columns%5B8%5D%5Borderable%5D=true&columns%5B8%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B8%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B8%5D%5Btype%5D=string&columns%5B8%5D%5BignoreFilter%5D=false&columns%5B8%5D%5Bdefault%5D=&columns%5B9%5D%5Bdata%5D=tipoPersonal&columns%5B9%5D%5Bname%5D=tipoPersonal&columns%5B9%5D%5Bsearchable%5D=true&columns%5B9%5D%5Borderable%5D=true&columns%5B9%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B9%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B9%5D%5Btype%5D=string&columns%5B9%5D%5BignoreFilter%5D=false&columns%5B9%5D%5Bdefault%5D=&columns%5B10%5D%5Bdata%5D=lugar&columns%5B10%5D%5Bname%5D=&columns%5B10%5D%5Bsearchable%5D=true&columns%5B10%5D%5Borderable%5D=true&columns%5B10%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B10%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B10%5D%5Btype%5D=string&columns%5B10%5D%5BignoreFilter%5D=false&columns%5B10%5D%5Bdefault%5D=&columns%5B11%5D%5Bdata%5D=montoPresupuestado&columns%5B11%5D%5Bname%5D=monto&columns%5B11%5D%5Bsearchable%5D=true&columns%5B11%5D%5Borderable%5D=true&columns%5B11%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B11%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B11%5D%5Btype%5D=number-range&columns%5B11%5D%5BignoreFilter%5D=true&columns%5B11%5D%5Bdefault%5D=&columns%5B12%5D%5Bdata%5D=montoDevengado&columns%5B12%5D%5Bname%5D=monto&columns%5B12%5D%5Bsearchable%5D=true&columns%5B12%5D%5Borderable%5D=true&columns%5B12%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B12%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B12%5D%5Btype%5D=number-range&columns%5B12%5D%5BignoreFilter%5D=true&columns%5B12%5D%5Bdefault%5D=&columns%5B13%5D%5Bdata%5D=pasajesViaticos&columns%5B13%5D%5Bname%5D=monto&columns%5B13%5D%5Bsearchable%5D=true&columns%5B13%5D%5Borderable%5D=true&columns%5B13%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B13%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B13%5D%5Btype%5D=number-range&columns%5B13%5D%5BignoreFilter%5D=false&columns%5B13%5D%5Bdefault%5D=&start="+str(start)+"&length="+str(length)+"&search%5Bvalue%5D=&search%5Bregex%5D=false&rangeSeparator=~", headers=headers) as resp:
            data = json.loads(await resp.text())
            return data

def datanomina():
    body=[]
    db = connection()
    date = datetime.datetime.now()
    year= date.year-1 if date.month==1 else date.year
    loop = asyncio.get_event_loop()
    query={
        "_id":1,
        "date":date,
        "month":1,
        "year":year,
        "number":0
    }
    
    db.data.update_one(
        { "_id": 1 },
        {
            "$set": {
                "date":date,
                "month":1,
                "year":year,
                "number":0
            }
        },True)

    for m in range(13):
        start=0
        loops = math.ceil(loop.run_until_complete(nomina(year, m+1, start, 1))['recordsFiltered']/1000)
        for i in range(loops):
            for d in loop.run_until_complete(nomina(year, m+1, start, 1000))['data']:
                body.append({
                    "awesomeId": str(d['anio'])+'-'+str(d['mes'])+'-'+str(d['codigoNivel'])+'-'+str(d['codigoEntidad'])+'-'
                    +str(d['codigoPersona'])+'-'+str(d['montoPresupuestado'])+'-'+str(d['montoDevengado']),
                    "anio": d['anio'], 
                    "mes": d['mes'], 
                    "codigoNivel": d['codigoNivel'], 
                    "descripcionNivel": d['descripcionNivel'], 
                    "codigoEntidad": d['codigoEntidad'], 
                    "descripcionEntidad": d['descripcionEntidad'], 
                    "codigoPersona": d['codigoPersona'], 
                    "nombres": d['nombres'], 
                    "apellidos": d['apellidos'], 
                    "sexo": d['sexo'], 
                    "discapacidad": d['discapacidad'], 
                    "tipoPersonal": d['tipoPersonal'], 
                    "lugar": d['lugar'], 
                    "montoPresupuestado": d['montoPresupuestado'], 
                    "montoDevengado": d['montoDevengado'], 
                    "pasajesViaticos": d['pasajesViaticos'], 
                    "mesCorte": d['mesCorte'], 
                    "anioCorte": d['anioCorte'], 
                    "fechaCorte": d['fechaCorte']
                })

            start+=1000
            try:
                db.data.update_one(query,{
                    "$set":{
                        "month":m+1,
                        "year":year,
                        "number":start
                    }
                })
                db.nomina.insert_many(body)
                db.nomina.create_index("awesomeId", unique=True)
            except pymongo.errors.DuplicateKeyError:
                pass
            except BulkWriteError as exc:
                exc.details
            body.clear()