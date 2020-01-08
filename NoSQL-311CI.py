from flask import Flask, request, abort

import json

import helpers
import queries

app = Flask(__name__)
appname = '/nosql/'


@app.route(appname+'query1', methods=['POST'])
def query1_listener():

    try:
        data = request.get_json()
        fr = helpers.strtodatetime(data['from'])
        to = helpers.strtodatetime(data['to'])
    except Exception as e:
        return json.dumps({'error':str(e)}, default=str), 400, { 'content-type':'application/json'}

    return json.dumps(queries.query1(fr, to), default=str), 200, { 'content-type':'application/json'}


@app.route(appname+'query2', methods=['POST'])
def query2_listener():

    try:
        data = request.get_json()
        fr = helpers.strtodatetime(data['from'])
        to = helpers.strtodatetime(data['to'])
        typ = data['type']
        if not isinstance(typ, str):
            raise ValueError('Type must be a string.')
    except Exception as e:
        return json.dumps({'error':str(e)}, default=str), 400, { 'content-type':'application/json'}

    return json.dumps(queries.query2(fr, to, typ), default=str), 200, { 'content-type':'application/json'}


@app.route(appname+'query3', methods=['POST'])
def query3_listener():

    try:
        data = request.get_json()
        fr, to = helpers.date_to_fromto(data['day'])
    except Exception as e:
        return json.dumps({'error':str(e)}, default=str), 400, { 'content-type':'application/json'}

    return json.dumps(queries.query3(fr, to), default=str), 200, { 'content-type':'application/json'}


@app.route(appname+'query4/', methods=['POST'])
def query4_listener():

    try:
        data = request.get_json()
        fr = helpers.strtodatetime(data['from'])
        to = helpers.strtodatetime(data['to'])
    except Exception as e:
        return json.dumps({'error':str(e)}, default=str), 400, { 'content-type':'application/json'}

    return json.dumps(queries.query4(fr, to), default=str), 200, { 'content-type':'application/json'}


@app.route(appname+'query5/', methods=['GET', 'POST'])
def query5_listener():

    return json.dumps(queries.query5(), default=str), 200, { 'content-type':'application/json'}


@app.route(appname+'query6/', methods=['GET', 'POST'])
def query6_listener():

    return json.dumps(queries.query6(), default=str), 200, { 'content-type':'application/json'}


@app.route(appname+'query7/', methods=['POST'])
def query7_listener():

    try:
        data = request.get_json()
        fr, to = helpers.date_to_fromto(data['day'])
    except Exception as e:
        return json.dumps({'error':str(e)}, default=str), 400, { 'content-type':'application/json'}

    return json.dumps(queries.query7(fr, to), default=str), 200, { 'content-type':'application/json'}


@app.route(appname+'query8/', methods=['GET', 'POST'])
def query8_listener():

    return json.dumps(queries.query8(), default=str), 200, { 'content-type':'application/json'}


@app.route(appname+'query9/', methods=['GET', 'POST'])
def query9_listener():

    return json.dumps(queries.query9(), default=str), 200, { 'content-type':'application/json'}


@app.route(appname+'query10/', methods=['GET', 'POST'])
def query10_listener():

    return json.dumps(queries.query10(), default=str), 200, { 'content-type':'application/json'}


@app.route(appname+'query11/', methods=['GET', 'POST'])
def query11_listener():

    data = request.get_json()
    try:
        username = data['username']
    except Exception as e:
        return json.dumps({'error':str(e)}, default=str), 400, { 'content-type':'application/json'}

    return json.dumps(queries.query11(username), default=str), 200, { 'content-type':'application/json'}

@app.route(appname+'admin/<username>', methods=['GET', 'POST'])
def adminbyusername_listener(username):

    adm = queries.getadminbyusername(username)
    if len(adm) == 0:
        return json.dumps({}), 404, { 'content-type':'application/json'}
    else:
        return json.dumps(adm[0], default=str), 200, { 'content-type':'application/json'}



@app.route(appname+'newlog/', methods=['POST'])
def newlog_listener():
    
    data = request.get_json()
    try:
        log = data['log']
        helpers.validate_log(log)
    except Exception as e:
        return json.dumps({'error':str(e)}, default=str), 400, { 'content-type':'application/json'}

    return json.dumps(queries.newlog(log), default=str), 201, { 'content-type':'application/json'}

@app.route(appname+'newupvote/', methods=['POST'])
def newupvote_listener():
    
    data = request.get_json()
    try:
        admin = data['admin_id']
        log = data['log_id']
    except Exception as e:
        json.dumps({'error':str(e)}, default=str), 400, { 'content-type':'application/json'}

    result = queries.newupvote(admin, log)

    if result.modified_count == 1:
        return {"updated": True}, 201, {'content-type':'application/json'}
    else:
        return {"updated": False}, 409, {'content-type':'application/json'}