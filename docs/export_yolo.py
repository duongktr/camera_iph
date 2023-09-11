from ultralytics import YOLO

# Load a model
model = YOLO("yolov8n.pt")  # load a pretrained model (recommended for training)

# Use the model
path = model.export(format="onnx", dynamic=True, opset=11)  # export the model to ONNX format