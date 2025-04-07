import os

import numpy as np
import tensorflow
import tf2onnx
import onnx
from tensorflow import keras

os.makedirs('models_output', exist_ok=True)

model_name = "acai_h.d1_dnn_20201130.h5"
model = keras.models.load_model(f'models_input/{model_name}')

metadata_input = np.zeros((1,25))
triplet_input = np.zeros((1, 63, 63, 3))

prediction = model([metadata_input, triplet_input])

print(f"TF score: {prediction}")

onnx_model, _ = tf2onnx.convert.from_keras(model)

#print(dir(onnx_model))
#print(onnx_model.opset_import)
#print(onnx_model.graph)


onnx.save(onnx_model, f'models_output/{model_name.replace(".h5", ".onnx")}')

