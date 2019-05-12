import shutil
import math
import string
import io
from io import BytesIO
import os
from os import path
import sys
import traceback
import boto
import boto.s3.connection
import datetime
from filechunkio import FileChunkIO
import threading
import queue
import time

user = 'rgw-RTMPuser'
access_key = 'V1B677DQF6JF38Z22Q0A'
secret_key = 'ssCZEQ0puNvPKA7GxSaxOkVYarfXiJn9rjjZk5xL'

filepath = "F:\\桌面\\S3-Test\\2019-01-04\\OK\\114544.flv"
keyname = "114544.flv"

threadcnt = 16

class Chunk:
    num = 0
    offset = 0
    len = 0
    def __init__(self, n, o, l):  
        self.num = n
        self.offset = o
        self.len = l

chunksize = 16 << 20
print("Data Size {}".format(chunksize))

def init_queue(filesize):
    chunkcnt = int(math.ceil(filesize*1.0/chunksize))
    print(chunkcnt)
    q = queue.Queue(maxsize = chunkcnt)
    for i in range(0,chunkcnt):
        offset = chunksize*i
        len = min(chunksize, filesize-offset)
        c = Chunk(i+1, offset, len)
        q.put(c)
    return q

def upload_chunk(filepath, mp, q, id):
#    print("thread id {} start".format(id) )
    while (not q.empty()):
#        print("thread id {} loop".format(id) )
        chunk = q.get()
        fp = FileChunkIO(filepath, 'r', offset=chunk.offset, bytes=chunk.len)
        mp.upload_part_from_file(fp, part_num=chunk.num)
        fp.close()
        q.task_done()
#    print("thread id {} exit".format(id) )

def upload_file_multipart(filepath, keyname, bucket, threadcnt=8):
    filesize = os.stat(filepath).st_size
    mp = bucket.initiate_multipart_upload(keyname)
    q = init_queue(filesize)
    for i in range(0, threadcnt):
        t = threading.Thread(target=upload_chunk, args=(filepath, mp, q, i))
        t.setDaemon(True)
        t.start()
    q.join()
    mp.complete_upload()

def set_bucket_name():
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
    return year + "-" + month + "-" + day + "-rtmp"

def bucket_check(conn):
    bucket_name = ''
    bucket_delete_count = '0'
    bucket_list = []
    bucket_list_reverse = []
    print("Bucket Check")
    bucket_count = len(conn.get_all_buckets())
#    print(bucket_count)
    if (bucket_count >= 15):
        print("Delete Bucket")
        for bucket in conn.get_all_buckets(): bucket_list.append(bucket.name)
        bucket_list_reverse = sorted(bucket_list, reverse=True)
        print(bucket_list_reverse)
        for x in range(1, bucket_count - 14):
            delete_bucket_name = bucket_list_reverse[bucket_count - x]
            print(delete_bucket_name + " ==> Delete")
            bucket = conn.get_bucket(delete_bucket_name)
            for key in bucket.list():
                print("  |==> Delete data ==> ", key)
                key.delete()
            conn.delete_bucket(delete_bucket_name)
            bucket_delete_count = str(x)
        print("Total Delete Bucket Count : " + bucket_delete_count)
    else:
        pass
    bucket_name = set_bucket_name()
    print("New Bucket Name ==> " + set_bucket_name())
    try: 
        bucket = conn.get_bucket(bucket_name)
        print("bucket exist")
    except: 
        bucket = conn.create_bucket(bucket_name)
        print("create bucket")
    return bucket


conn = boto.connect_s3(
    aws_access_key_id = access_key,
    aws_secret_access_key = secret_key,
    host = '192.168.3.1', port=7480,
    is_secure=False,
    calling_format = boto.s3.connection.OrdinaryCallingFormat(),
)

bucket = bucket_check(conn)
print(datetime.datetime.now())

'''
for x in range(1, 31):
    if x < 10: y = "0" + str(x)
    else: y = str(x)
    conn.create_bucket("2019-03-" + str(y) + "-rtmp")
#    conn.delete_bucket("2019-03-" + str(x) + "-rtmp")
print("End Bucket Setup")
'''
'''
for x in range(1, 101):
    if x < 10: y = "0" + str(x)
    else: y = str(x)
    key = bucket.new_key('hello' + y + '.txt')
    key.set_contents_from_string('Hello World!')
'''
# print(conn.get_all_buckets())

time1= time.time()
upload_file_multipart(filepath, keyname, bucket, threadcnt)
time2= time.time()
print ("upload %s with %d threads use %d seconds" % (keyname, threadcnt, time2-time1))

# key = bucket.new_key('2019-01-04_OK.flv')
# key.set_contents_from_filename('F:\\桌面\\S3-Test\\2019-01-04\\OK\\114544.flv')

print("List Bucket")
for list_bucket in conn.get_all_buckets():
    print ("{name}\t{created}".format(
            name = list_bucket.name,
            created = list_bucket.creation_date
            )
    )

print("Bucket Data")
for key in bucket.list():
        print ("{name}\t{size}\t{modified}".format(
                name = key.name,
                size = key.size,
                modified = key.last_modified
                )
        )
