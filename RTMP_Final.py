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

RTMP_DUMP_HEADER = "rtmpdump"
RTMP_HEADER = "rtmp://"
RTMP_IP = "10.0.0.174"
RTMP_PORT = "1935"
RTMP_URL_HEADER = "A0 - 0 - 0 - 0 - "
IPCAM_ACCOUNT = "admin"
IPCAM_PASSWORD = "888888"
convert_locate = "G:\\IPCAM_Stream_To_File\\Video-File\\"
pre_record_day = ""

def remove_dir():
    global convert_locate
    all_dir = sorted(os.listdir(convert_locate))
#    print(all_dir)
#   delete the oldest file dir
    os.system("rm -rf " + convert_locate + all_dir[0])
    print("rm dir = " + all_dir[0])

def check_disk():
    global convert_locate
    obj_disk = psutil.disk_usage(convert_locate)
#    total_disk = round(obj_disk.total / (1024.0 ** 3), 3)
#    usage_disk = round(obj_disk.used / (1024.0 ** 3), 3)
#    free_disk = round(obj_disk.free / (1024.0 ** 3), 3)
    usage_persent_disk = obj_disk.percent
    if ((100 - usage_persent_disk) < 20):
#        print("Disk full alert")
        remove_dir()
#    print("Total : " + str(total_disk) + " GB")
#    print("Usage : " + str(usage_disk) + " GB")
#    print("Free  : " + str(free_disk) + " GB")
#    print("Usage : " + str(usage_persent_disk) + " %")

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
    if not os.path.isdir(store_locate):
        os.mkdir(store_locate)

    store_locate = store_locate + "\\"

    if not os.path.isdir(store_locate + "\\" + "OK"):
        os.mkdir(store_locate + "\\" + "OK")
    
    if not os.path.isdir(store_locate + "\\" + "Backup"):
        os.mkdir(store_locate + "\\" + "Backup")

    return store_locate, hour+minute+second

#print(filename()[0], filename()[1])

while (1):
    check_disk()
    generate_uuid = uuid.uuid4()
    user_uuid_A = str(generate_uuid).upper().replace('-', '')
    generate_uuid = uuid.uuid4()
    user_uuid_B = str(generate_uuid).upper().replace('-', '')
    convert_locate_A = filename()[0] + "OK\\" + filename()[1]
    convert_locate_B = filename()[0] + "Backup\\" + filename()[1]
#    print(convert_locate_A, convert_locate_B)
    send_command_OK = RTMP_DUMP_HEADER + ' -r "' + RTMP_HEADER + RTMP_IP + ":" + RTMP_PORT + "/" + '" -y "' + RTMP_URL_HEADER + user_uuid_A + " - " + IPCAM_ACCOUNT + " - " + IPCAM_PASSWORD + '" -Y -o ' + convert_locate_A + '.flv -v'
    send_command_Backup = RTMP_DUMP_HEADER + ' -r "' + RTMP_HEADER + RTMP_IP + ":" + RTMP_PORT + "/" + '" -y "' + RTMP_URL_HEADER + user_uuid_B + " - " + IPCAM_ACCOUNT + " - " + IPCAM_PASSWORD + '" -Y -o ' + convert_locate_B + '.flv -v'
    print(send_command_OK)
#    print(send_command_Backup)
#    record_Backup = subprocess.Popen(send_command_Backup)
    time.sleep(0.01)
    record_OK = subprocess.Popen(send_command_OK)
    time.sleep(300)
#    record_Backup.kill()
    time.sleep(0.01)
    record_OK.kill()
#   kill backup file    
#    print("File Dir == " + filename()[0])
    if (pre_record_day == ""):
#        print("\n Get pre_record_day")
        pre_record_day = filename()[0]
#    else: print("\n" + pre_record_day)
    if (pre_record_day != filename()[0]):
        os.system("rm -r " + pre_record_day + "Backup\\")
        pre_record_day = filename()[0]