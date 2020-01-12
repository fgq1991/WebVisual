#encoding: utf-8
from flask import Flask,render_template,request
import GenerateElasticSearchVisualization
from pymongo import MongoClient

app = Flask(__name__)

@app.route('/')
def index():
    title = 'Dashboard'
    return render_template('main.html',title=title)

@app.route('/draw/',methods=['POST'])
def draw_handler():
    db_name = 'twitter_demographics_across_followers'
    host = 'localhost'
    port = 27017
    client = MongoClient(host, port)  # connect to MongoDB server
    db = client[db_name]
    screenname = request.form.get('screenname')
    return GenerateElasticSearchVisualization.writeMultipleUserInfo(db, screenname)

if __name__ == '__main__':
    app.run(debug=True,port=1337)