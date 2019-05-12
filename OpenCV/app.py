import cv2
cap = cv2.VideoCapture()
cap.open('rtsp://10.0.0.174:8554/channel=0/subtype=0/vod=20180921-123456')

while True:
    frame,err = cap.read()
    test = cv2.resize(err, (1366, 768))
    cv2.imshow('Stream', test)
    if cv2.waitKey(1) == 27:
        cap.release()
        cv2.destroyAllWindows()
        break