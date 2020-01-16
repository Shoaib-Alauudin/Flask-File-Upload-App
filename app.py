import pandas as pd
from flask import render_template, flash, request, redirect, url_for, jsonify
from werkzeug import secure_filename
from . import app_run
from Connection import Connection
import os

# uploads file directory
UPLOAD_FOLDER = './uploads'

# upload directory file config
app_run.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Only upload csv files
ALLOWED_EXTENSIONS = set(['csv'])

# Db connection
connection = Connection()


@app_run.errorhandler(404)
def invalid_route(e):
    return jsonify({'errorCode' : 404, 'message' : 'Route not found'})

@app_run.route('/', methods=['GET'])
def home():

    # sample dataset
    data = [[37390, "2019-06-05 23:08:31.407799","1540.24668631379","0 days 00:27:41.382812755"],
            [37392,"2019-06-05 23:08:33.407799","1529.07676087828","0 days 00:34:14.439860088"],
            [37395,"2019-06-05 23:08:36.407799","1565.64643628509","0 days 00:31:19.217825034"],
            ["37405","2019-06-05 23:08:46.407799","1552.63246994307","0 days 00:26:42.045578665"]]
    sample_df = pd.DataFrame(data=data, columns=['id', 'timestamp', 'temperature', 'duration'])
    return render_template("base.html", data=sample_df.to_html())

@app_run.route('/database/', methods=['GET'])
def db_to_html():
    # query for grab all records
    query = 'select * from csv_task;'
    db_df = pd.read_sql(query, connection.getConnection())
    return render_template("index.html", data=db_df.to_html())


@app_run.route('/file_upload/', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':

        # check if the post request has the file part
        if 'file' not in request.files:
            # flash('No file part')
            return redirect(request.url)
        file = request.files['file']

        # if user does not select file, browser also
        # submit an empty part without filename
        if file.filename == '':
            # flash('No selected file')
            return render_template("file_upload.html")

        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file_path = os.path.join(app_run.config['UPLOAD_FOLDER'], filename)
            file.save(file_path)

            csv_df = pd.read_csv(file_path, sep=',', quotechar='\'', encoding='utf8', index_col=False)

            db_cols = ['id', 'timestamp', 'temperature', 'duration']
            csv_cols = [col.lower() for col in csv_df.columns if col.lower() in db_cols]

            if csv_cols == db_cols:

                # read database records
                query = 'select * from csv_task;'

                db_df = pd.read_sql(query, connection.getConnection())

                new_df = csv_df[~csv_df['id'].isin(db_df['id'])].dropna()

                if not new_df.empty:
                    # Write dataframe into dataframe
                    new_df.to_sql('csv_task', con=connection.getConnection(), index=False,
                                  if_exists='append')  # Replace Table_name with your sql table name

            return redirect(url_for("db_to_html",
                                    filename=filename))
    return render_template("file_upload.html")



def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS