import os
import glob
import ast
import time
from datetime import datetime
import pandas as pd
import numpy as np
from joblib import dump
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report

from reach.feature_extraction import extract_features


# 随机森林模型，长臂判断，需要读取tick，ping，rotation，distance

def load_dataset():
    """
    从data文件夹中加载csv，然后特征提取。
    """
    data_list = []

    # 加载作弊对局，label=1
    hack_files = glob.glob("data/processCSV/hack/*.csv")
    print(f"Found {len(hack_files)} csv files in /hack.")
    for file in hack_files:
        feat = extract_features(file)
        if feat is not None:
            feat['label'] = 1
            feat['file_path'] = file
            data_list.append(feat)

    # 加载正常对局 ，label=0
    normal_files = glob.glob("data/processCSV/normal/*.csv")
    print(f"Found {len(normal_files)} csv files in /normal.")
    for file in normal_files:
        feat = extract_features(file)
        if feat is not None:
            feat['label'] = 0
            feat['file_path'] = file
            data_list.append(feat)

    if not data_list:
        raise ValueError("File errors")

    df = pd.DataFrame(data_list)
    return df

def main():
    data = load_dataset()
    print("Number of records: ", len(data))
    X = data.drop(columns=['label', 'file_path'])
    y = data['label']
    # 分两成出来当测试集
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

    clf = RandomForestClassifier(n_estimators=100, random_state=42)
    clf.fit(X_train, y_train)

    y_pred = clf.predict(X_test)
    print("For default threshold (0.5):")
    print(classification_report(y_test, y_pred))
    # 导出
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"model/reach_detect_model_{timestamp}.joblib"
    dump(clf, filename)
    print(f"Model exported to {filename}")

if __name__ == "__main__":
    main()
