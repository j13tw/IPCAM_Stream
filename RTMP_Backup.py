import os, sys
import datetime
import time
import uuid
import subprocess

def filename():
    convert_locate = "G:/"
    year = str(datetime.datetime.now().year)
    month = str(datetime.datetime.now().month)
    if (int(month) < 10): month = "0" + str(month)
    day = str(datetime.datetime.now().day)
    if (int(day) < 10): day = "0" + str(day)
    hour = str(datetime.datetime.now().hour)
    if (int(hour) < 10): hour = "0" + str(hour)
    minute = str(datetime.datetime.now().minute)
    if (int(minute) < 10): minute = "0" + str(minute)
    second = str(datetime.datetime.now().second)
    if (int(second) < 10): second = "0" + str(second)
    #print(year, month, day, hour, minute, second)

    convert_locate = convert_locate + year + "-" + month 
    if not os.path.isdir(convert_locate):
        os.mkdir(convert_locate)

    convert_locate = convert_locate + "/" + day 
    if not os.path.isdir(convert_locate):
        os.mkdir(convert_locate)

    convert_locate = convert_locate + "/"

    if not os.path.isdir(convert_locate + "/" + "OK"):
        os.mkdir(convert_locate + "/" + "OK")
    
    if not os.path.isdir(convert_locate + "/" + "Backup"):
        os.mkdir(convert_locate + "/" + "Backup")

    return convert_locate, hour+minute+second

#print(filename()[0], filename()[1])

RTMP_DUMP_HEADER = "rtmpdump"
RTMP_HEADER = "rtmp://"
RTMP_IP = "10.0.0.174"
RTMP_PORT = "1935"
RTMP_URL_HEADER = "A0 - 0 - 0 - 0 - "
IPCAM_ACCOUNT = "admin"
IPCAM_PASSWORD = "888888"

generate_uuid = uuid.uuid4()
user_uuid = str(generate_uuid).upper().replace('-', '')

while (1):
    convert_locate = filename()[0] + "Backup/" + filename()[1]
    send_command = RTMP_DUMP_HEADER + ' -r "' + RTMP_HEADER + RTMP_IP + ":" + RTMP_PORT + "/" + '" -y "' + RTMP_URL_HEADER + user_uuid + " - " + IPCAM_ACCOUNT + " - " + IPCAM_PASSWORD + '" -Y -o ' + convert_locate + '.flv -v'
    print(send_command)
    record_Backup = subprocess.Popen(send_command, shell = True)
    time.sleep(60)
    record_Backup.kill()