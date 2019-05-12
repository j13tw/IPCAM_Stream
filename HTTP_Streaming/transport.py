import os, sys
import datetime
import time
import subprocess
import uuid
import psutil
import shutil

# 8F IP & port
# 10.0.0.174:1935

# extend IP & Port
# 211.20.7.115:31935

RTMP_GW_HEADER = "rtmpgw"
RTMP_GW_VARIABLE = " -v -g "
RTMP_HEADER = "rtmp://"
RTMP_IP = "211.20.7.115"
RTMP_PORT = "31935"
RTMP_URL_HEADER = "A0 - 0 - 0 - 0 - "
IPCAM_ACCOUNT = "admin"
IPCAM_PASSWORD = "888888"

HTTP_PORT = "1935"


def filename():
    global convert_locate
    year = str(datetime.datetime.now().year)
    month = datetime.datetime.now().month
    if (month < 10): month = "0" + str(month)
    else: month = str(month)
    day = datetime.datetime.now().day
    if (day < 10): day = "0" + str(day)
    else: day = str(day)
    hour = datetime.datetime.now().hour
    if (hour < 10): hour = "0" + str(hour)
    else: hour = str(hour)
    minute = datetime.datetime.now().minute
    if (minute < 10): minute = "0" + str(minute)
    else: minute = str(minute)
    second = datetime.datetime.now().second
    if (second < 10): second = "0" + str(second)
    else: second = str(second)
    #print(year, month, day, hour, minute, second)

    store_locate = convert_locate + year + "-" + month + "-" + day
    return hour+minute+second, 

#print(filename()[0], filename()[1])

generate_uuid = uuid.uuid4()
user_uuid = str(generate_uuid).upper().replace('-', '')
send_command = RTMP_GW_HEADER + ' -r "' + RTMP_HEADER + RTMP_IP + ":" + RTMP_PORT + "/" + '" -y "' + RTMP_URL_HEADER + user_uuid + " - " + IPCAM_ACCOUNT + " - " + IPCAM_PASSWORD + '"' + RTMP_GW_VARIABLE + HTTP_PORT 
print(send_command)
os.system(send_command)