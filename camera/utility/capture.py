from loguru import logger
import pickle
import redis
import time
import cv2
import os


def read_frame(s):
    redis_client = redis.Redis.from_url('redis://localhost:6379/0')
    max_queue = int(os.getenv('MAX_QUEUE', '10'))
    batch_size = int(os.getenv('BATCH_SIZE', '10'))

    cap = cv2.VideoCapture(0 if s == '0' else s)
    # assert cap.isOpened(), 'Failed to open %s' % s
    w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    fps = cap.get(cv2.CAP_PROP_FPS) % 100
    logger.info("Load video: {}, resolution: {}, fps: {}".format(
        s, (w, h), fps
    ))
    n = 0
    while True:
        ret, frame = cap.read()
        if not ret:
            cap = cv2.VideoCapture(0 if s == '0' else s)
            ret, frame = cap.read()

        # count
        n += 1
        if n != batch_size:  # read every batch frame
            continue

        if redis_client.llen(s) < max_queue:
            redis_client.rpush(s, pickle.dumps(frame))
        n = 0
        time.sleep(0.01)  # wait time