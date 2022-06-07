from kafka import KafkaConsumer
import csv
from datetime import datetime
import os

import cv2
import numpy as np
import cryptocode

import yoloDetect


consumer = KafkaConsumer('test', bootstrap_servers='localhost:9092')
cryptedImg = "" 

for message in consumer:
	msg = str(message.value.decode())


	if msg == "Son":
		cryptedImg_decoded = cryptocode.decrypt(cryptedImg, "NurUmmu")

		fromStringArray = np.fromstring(cryptedImg_decoded[1:-2], dtype=np.uint8, sep=',', count=921600)

		framereshape = fromStringArray.reshape((480, 640, 3))
		frameYolo = np.copy(framereshape)
		detectedFrame = yoloDetect.findObject(frameYolo)

		cv2.imshow('Detected Frame', detectedFrame)
		cv2.imshow('Default Frame', framereshape)

		if cv2.waitKey(1) == ord('q'):
			break
		
		cryptedImg = ""
		continue


	cryptedImg += msg

