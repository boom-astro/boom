#!/usr/bin/env -S uv run --script
# /// script
# requires-python = "==3.10.*"
# dependencies = ["numpy", "tensorflow==2.13", "tf2onnx", "onnx"]
# ///
"""Convert TensorFlow Keras models to ONNX format.

To run this script, export models to the ``models_input`` directory
(relative to this script) and call (from the root of the repository):

    uv run scripts/convert2onnx/convert2onnx.py
"""

import os
import glob

import tensorflow
import tf2onnx
import onnx
from tensorflow import keras


def convert2onnx(model_name):
    print(f"Converting {model_name}...")
    model = keras.models.load_model(f"models_input/{model_name}")
    onnx_model, _ = tf2onnx.convert.from_keras(model)

    output_path = None

    if os.path.isdir(f"models_input/{model_name}"):
        output_path = f"models_output/{model_name.rstrip('/')}.onnx"
    elif model_name.endswith(".h5"):
        output_path = f"models_output/{model_name.replace('.h5', '.onnx')}"
    else:
        raise ValueError(f"Unsupported model format: {model_name}")
    onnx.save(onnx_model, output_path)


def get_model_names():
    # h5 models end in .h5
    h5_models = glob.glob("models_input/*.h5")
    # pb models are directories
    pb_models = glob.glob("models_input/*/")
    # remove "models_input/"
    return [
        model.replace("models_input/", "") for model in h5_models + pb_models
    ]


def convert_all_models():
    model_names = get_model_names()
    print(f"Converting {len(model_names)} models...")
    for model_name in model_names:
        convert2onnx(model_name)


def main():
    convert_all_models()


if __name__ == "__main__":
    # Move into this directory to ensure relative paths work correctly
    os.chdir(os.path.dirname(__file__))
    os.makedirs("models_output", exist_ok=True)
    main()
