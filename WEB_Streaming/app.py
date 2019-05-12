#!/usr/bin/python
import requests
import os, sys
import uuid
from datetime import datetime, timedelta	
from flask import Flask, request, url_for, redirect, jsonify
from flask import render_template

app = Flask(__name__)

@app.route('/', methods=['GET'])
def rtmp():
    RTMP_HEADER = "rtmp://"
    RTMP_IP = "211.20.7.115"
    RTMP_PORT = "31935"
    RTMP_URL_HEADER = "A0 - 0 - 0 - 0 - "
    IPCAM_ACCOUNT = "admin"
    IPCAM_PASSWORD = "888888"

    generate_uuid = uuid.uuid4()
    user_uuid = str(generate_uuid).upper().replace('-', '')
    rtmp_url = RTMP_HEADER + RTMP_IP + ":" + RTMP_PORT + "/" + RTMP_URL_HEADER + user_uuid + " - " + IPCAM_ACCOUNT + " - " + IPCAM_PASSWORD
    print(rtmp_url)
    return render_template('rtmp.html', rtmp_url = rtmp_url)

if __name__ == '__main__':
#	app.run(debug = True)
	app.run(host = '0.0.0.0', port = 3000)
	