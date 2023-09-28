from time import strftime, localtime, time
import numpy as np
import base64
from PIL import Image
import tempfile
import io
from datetime import date

def format_global_id(number: int):
    s = date.today()
    return s.replace("-",) + "-" +number #yyyy-mm-dd-id
def convert_second_2_datetime(seconds):
    return strftime('%Y-%m-%d %H:%M:%S', localtime(seconds))

def split_datetime(datetime: str):
    d, t = datetime.split(" ") #yyyy-mm-dd
    return d, t  #yyyymmdd

def split_prefix_id(global_id):
    return int(global_id[8:]) #prefix yyyymmdd

def convert_bytes_to_numpy_array(j_dumps: str, resize=False) -> np.array:
    # TODO: load json string to numpy array
    compressed_data = base64.b64decode(j_dumps)
    img = Image.open(io.BytesIO(compressed_data))

    # convert PNG -> JPEG
    img_path = tempfile.NamedTemporaryFile(suffix='.jpg').name
    img.convert("RGB").save(img_path, "JPEG")
    img = Image.open(img_path)

    # resize with aspect ratio
    if resize:
        new_width = float(500)
        if max([img.height, img.width]) > new_width:
            aspect_ratio = img.height / img.width
            new_height = new_width * aspect_ratio
            img = img.resize((int(new_width), int(new_height)))

    return np.array(img)