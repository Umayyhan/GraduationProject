from itertools import count
from time import sleep
from kafka import KafkaProducer
import cv2
import numpy as np
import cryptocode

producer = KafkaProducer(bootstrap_servers = 'localhost:9092')

cap = cv2.VideoCapture(0)
if not cap.isOpened():
    print("Cannot open camera")
    exit()

while True:
    # Capture frame-by-frame
	ret, frame = cap.read()
	framereshape = frame.reshape((1,921600))
	framereshapeList = framereshape.tolist()
	framereshapeStr = ""

	for i in framereshapeList:
		framereshapeStr += str(i)
		framereshapeStr += ","

	"""framereshapeStr = np.array_str(framereshape)	
	print(framereshapeStr[2:-2])"""


	str_encoded = cryptocode.encrypt(framereshapeStr,"NurUmmu")


	"""str_decoded = cryptocode.decrypt(str_encoded, "NurUmmu")

	fromStringArray = np.fromstring(str_decoded[:-1], dtype=np.uint8, sep=',', count=921600)

	framereshape = fromStringArray.reshape((480,640,3))

	cv2.imshow('frame', framereshape)
	if cv2.waitKey(1) == ord('q'):
		break"""

	future = producer.send('test', str_encoded[1000000 * 0:1000000 * 1].encode('utf-8'))
	future = producer.send('test', str_encoded[1000000 * 1:1000000 * 2].encode('utf-8'))
	future = producer.send('test', str_encoded[1000000 * 2:1000000 * 3].encode('utf-8'))
	future = producer.send('test', str_encoded[1000000 * 3:1000000 * 4].encode('utf-8'))
	future = producer.send('test', str_encoded[1000000 * 4:1000000 * 5].encode('utf-8'))
	future = producer.send('test', str_encoded[1000000 * 5:1000000 * 6].encode('utf-8'))

	future = producer.send('test', "Son".encode('utf-8'))

	sleep(3)

	#future = producer.send('test', str_encoded.encode('utf-8'))

"""
producer = KafkaProducer(bootstrap_servers = 'localhost:9092')


while True:
	print("\n\nType \"quit\" to exit")
	print("Enter message to be sent:")
	msg = input()
	if msg == "quit":
		print("Exiting...")
		break
	producer.send('test', msg.encode('utf-8'))
	print("Sending msg \"{}\"".format(msg))
	print("Message sent!")"""