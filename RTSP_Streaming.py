import rtsp
import datetime

# Get Streaming Date
year = str(datetime.datetime.now().year)
month = datetime.datetime.now().month
if (int(month) < 10): month = "0" + str(month)
day = str(datetime.datetime.now().day)
if (int(day) < 10): day = "0" + str(day)
hour = str(datetime.datetime.now().hour)
if (int(hour) < 10): hour = "0" + str(hour)
minute = str(datetime.datetime.now().minute)
if (int(minute) < 10): minute = "0" + str(minute)
second = str(datetime.datetime.now().second)
if (int(second) < 10): second = "0" + str(second)

# RTSP connection Setup
RTSP_SERVER = "10.0.0.174"
RTSP_PORT = "8554"
RTSP_API = "/channel=0/subtype=0/vod="
RTSP_URL = "rtsp://" + RTSP_SERVER + ":" + RTSP_PORT + RTSP_API + year + month + day + "-" + hour + minute + second

# RTSP Streaming connect
client = rtsp.Client(rtsp_server_uri = RTSP_URL)
client.preview()
client.close()