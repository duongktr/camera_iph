import cv2
import numpy as np
import pickle
from pathlib import Path
import redis

from boxmot import DeepOCSORT, BoTSORT
from ultralytics import YOLO


trackers = {}
detector = YOLO('yolov8n.pt')

color = (0, 0, 255)  # BGR
thickness = 2
fontscale = 0.5

client = redis.Redis.from_url('redis://localhost:6379/0')
# keys = client.keys('*')
# for key in keys:
#     key = key.decode()
#     trackers[key] = BoTSORT(
#         model_weights=Path('osnet_x0_25_msmt17.pt'), # which ReID model to use
#         device='cpu',
#         fp16=False,
#     )

while True:
    keys = client.keys('*')
    for key in keys:
        key = key.decode()

        if key not in trackers:
            trackers[key] = BoTSORT(
            model_weights=Path('osnet_x0_25_msmt17.pt'), # which ReID model to use
            device='cpu',
            fp16=False,
        )

        # Get data
        data = client.lpop(key)
        if data is None:
            continue
        im = pickle.loads(data)

        # substitute by your object detector, input to tracker has to be N X (x, y, x, y, conf, cls)
        # dets = np.array([[144, 212, 578, 480, 0.82, 0],
        #                 [425, 281, 576, 472, 0.56, 65]])
        # source = cv2.imread('/home/hoang/Downloads/aihub/tracking/data/warm_up_data/14b_0_105102/img1/000001.jpg')
        dets = detector.predict(source=im, classes=0)
        dets = dets[0].boxes.data.cpu().numpy()

        tracks = trackers[key].update(dets, im) # --> (x, y, x, y, id, conf, cls, ind)

        xyxys = tracks[:, 0:4].astype('int') # float64 to int
        ids = tracks[:, 4].astype('int') # float64 to int
        confs = tracks[:, 5]
        clss = tracks[:, 6].astype('int') # float64 to int
        inds = tracks[:, 7].astype('int') # float64 to int

        # in case you have segmentations or poses alongside with your detections you can use
        # the ind variable in order to identify which track is associated to each seg or pose by:
        # segs = segs[inds]
        # poses = poses[inds]
        # you can then zip them together: zip(tracks, poses)

        # print bboxes with their associated id, cls and conf
        if tracks.shape[0] != 0:
            for xyxy, id, conf, cls in zip(xyxys, ids, confs, clss):
                conf = round(conf, 4)
                im = cv2.rectangle(
                    im,
                    (xyxy[0], xyxy[1]),
                    (xyxy[2], xyxy[3]),
                    color,
                    thickness
                )
                cv2.putText(
                    im,
                    f'id: {id}, conf: {conf}, c: {cls}',
                    (xyxy[0], xyxy[1]-10),
                    cv2.FONT_HERSHEY_SIMPLEX,
                    fontscale,
                    color,
                    thickness
                )

        # show image with bboxes, ids, classes and confidences
        cv2.imshow(key, im)

        # break on pressing q
        if cv2.waitKey(1) & 0xFF == ord('q'):
            break
