from flask import request, Flask
import pandas as pd
from Connection import Connection
import datetime
from flask import g
import time
import logging

app_run = Flask(__name__)

# Db connection
connection = Connection()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

@app_run.before_request
def start_timer():
    g.start = time.time()

@app_run.after_request
def log_request(response):
    if request.path == '/favicon.ico':
        return response
    elif request.path.startswith('/templates'):
        return response

    now = time.time()
    duration = round(now - g.start, 2)
    dt = datetime.datetime.fromtimestamp(now)


    ip = request.headers.get('X-Forwarded-For', request.remote_addr)
    host = request.host.split(':', 1)[0]
    args = dict(request.args)

    request_id = request.headers.get('X-Request-ID')
    if response.status_code != 500:
        log_dict = {'method':request.method,'path':request.path, 'status':response.status_code,
                    'duration':duration, 'time':dt, 'ip':ip, 'host':host, 'params':str(args),
                    'request_id':request_id}
        log_df = pd.DataFrame([log_dict])

        if not log_df.empty:
            # Write dataframe into dataframe
            log_df.to_sql('task_logs', con=connection.getConnection(), index=False, if_exists='append')  # Replace Table_name with your sql table name

    return response