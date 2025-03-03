import os
import sys
import ast
import glob
import random
import pandas as pd
import numpy as np
from joblib import load

def extract_features_for_inference(csv_path):
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"Failed to read file {csv_path}: {e}")
        return None

    df['ping'] = df['Ping'].apply(lambda x: ast.literal_eval(x)['ping'] if pd.notnull(x) else np.nan)
    df['yaw'] = df['Rotation'].apply(lambda x: ast.literal_eval(x)['yaw'] if pd.notnull(x) else np.nan)
    df['pitch'] = df['Rotation'].apply(lambda x: ast.literal_eval(x)['pitch'] if pd.notnull(x) else np.nan)

    df[['Distance', 'ping', 'yaw', 'pitch']] = df[['Distance', 'ping', 'yaw', 'pitch']].fillna(0)

    features = {f'{col}_{stat}': getattr(df[col], stat)() for col in ['Distance', 'ping', 'yaw', 'pitch']
                for stat in ['mean', 'std', 'min', 'max']}

    features['num_ticks'] = len(df)
    return features


def load_latest_model(model_dir="model"):
    model_files = glob.glob(f"{model_dir}/*.joblib")
    if not model_files:
        print("No model files found.")
        return None

    latest_model = max(model_files, key=os.path.getmtime)
    try:
        clf = load(latest_model)
        print(f"Model loaded successfully: {latest_model}")
        return clf
    except Exception as e:
        print(f"Error loading model: {e}")
        return None


def process_csv_folder(csv_folder, model_dir="model", threshold=0.8):
    csv_files = glob.glob(f"{csv_folder}/*.csv")
    if not csv_files:
        print("No CSV files found in the folder.")
        return

    clf = load_latest_model(model_dir)
    if clf is None:
        print("Failed to load model. Exiting...")
        return

    for csv_file in csv_files:
        feat_dict = extract_features_for_inference(csv_file)
        if feat_dict is None:
            print(f"Skipping file {csv_file} due to extraction failure.")
            continue

        X_infer = pd.DataFrame([feat_dict])
        prob = clf.predict_proba(X_infer)[:, 1][0]
        prediction = 1 if prob >= threshold else 0

        print(f"File {csv_file}: Cheating probability: {prob:.4f}")


if __name__ == "__main__":
    csv_folder = "data/processCSV/normal"
    process_csv_folder(csv_folder)
