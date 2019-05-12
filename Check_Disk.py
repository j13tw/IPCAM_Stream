import os, sys
import psutil
import datetime

check_distance = "G:\\"

def check_disk():
    global check_distance
    obj_disk = psutil.disk_usage(check_distance)
    total_disk = round(obj_disk.total / (1024.0 ** 3), 3)
    usage_disk = round(obj_disk.used / (1024.0 ** 3), 3)
    free_disk = round(obj_disk.free / (1024.0 ** 3), 3)
    usage_persent_disk = obj_disk.percent
    if ((100 - usage_persent_disk) < 10):
        print("Disk full alert")
        remove_dir()
    print("Total : " + str(total_disk) + " GB")
    print("Usage : " + str(usage_disk) + " GB")
    print("Free  : " + str(free_disk) + " GB")
    print("Usage : " + str(usage_persent_disk) + " %")

def remove_dir():
    global check_distance
    all_dir = sorted(os.listdir(check_distance))
    print("all_dir = " + str(all_dir))
#    os.rmdir(check_disk + all_dir[0])
    print("rm dir = " + all_dir[0])

check_disk()