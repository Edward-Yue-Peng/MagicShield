import glob
import os
import re
from datetime import datetime

import pandas as pd
from joblib import dump
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
from sklearn.model_selection import train_test_split

# 你自己的特征提取函数
from reach.utils.feature_extraction import extract_features


def parse_replay_id(filename):
    """
    假设文件名类似 "20917698_segment_1.csv"，则提取 "20917698" 作为回放号。
    如果格式不符，默认返回原文件名。
    """
    base = os.path.basename(filename)
    match = re.match(r"(.*)_segment_\d+\.csv", base)
    if match:
        return match.group(1)
    else:
        return base


def load_segment_dataset(data_folder_path):
    """
    从 processed_csv/hack 和 processed_csv/normal 中加载所有 segment CSV。
    每个 segment CSV 被视为一个样本，使用 extract_features_in_chunks 进行特征提取。

    参数:
    - data_folder_path: 包含 processed_csv/hack 和 processed_csv/normal 的根目录
    - chunk_size=999999: 让一个文件只产生一个特征行（即整个文件视为一个chunk）

    返回: 包含特征、label、file_path、replay_id 的 DataFrame
    """
    data_list = []
    hack_path = os.path.join(data_folder_path, "data", "processed_csv", "hack", "*.csv")
    normal_path = os.path.join(data_folder_path, "data", "processed_csv", "normal", "*.csv")

    hack_files = glob.glob(hack_path)
    normal_files = glob.glob(normal_path)

    print(f"Found {len(hack_files)} hack segment files.")
    print(f"Found {len(normal_files)} normal segment files.")
    # 加载 hack segments
    for file in hack_files:
        feats = extract_features(file)
        # 如果 feats 返回 None 或空 DataFrame，则跳过
        if feats is not None and not feats.empty:
            # 由于 chunk_size 很大，理论上 feats 只会有一行
            feats['label'] = 1
            feats['file_path'] = file
            feats['replay_id'] = parse_replay_id(file)
            data_list.append(feats)

    # 加载 normal segments
    for file in normal_files:
        feats = extract_features(file)
        if feats is not None and not feats.empty:
            feats['label'] = 0
            feats['file_path'] = file
            feats['replay_id'] = parse_replay_id(file)
            data_list.append(feats)

    if not data_list:
        raise ValueError("No valid segment CSV files found or feature extraction failed.")

    return pd.concat(data_list, ignore_index=True)


def train_reach(data_folder_path, threshold=0.75):
    """
    训练模型:
    1) 每个 segment CSV 作为一个样本
    2) 训练后，对测试集做文件级别(回放号级别)的聚合判断:
       - 只要回放号里任意一个 segment 被预测为作弊，则整场判定为作弊
    3) 输出文件级别的分类报告并保存模型
    """
    # 1. 加载数据
    data = load_segment_dataset(data_folder_path)
    print("Number of total segment samples:", len(data))

    # 2. 分离特征与标签
    X = data.drop(columns=['label', 'file_path', 'replay_id'])
    y = data['label']
    file_paths = data['file_path']
    replay_ids = data['replay_id']

    # 3. 划分训练集和测试集
    (X_train, X_test, y_train, y_test,
     file_paths_train, file_paths_test,
     replay_ids_train, replay_ids_test) = train_test_split(
        X, y, file_paths, replay_ids,
        test_size=0.2, random_state=42, stratify=y
    )

    # 4. 训练模型
    clf = RandomForestClassifier(n_estimators=100, random_state=42)
    clf.fit(X_train, y_train)

    # 5. 测试集预测(段级别)
    y_prob = clf.predict_proba(X_test)[:, 1]  # 预测为正类(作弊)的概率
    y_pred_thresh = (y_prob >= threshold).astype(int)

    # 6. 构建段级别的结果
    df_results = pd.DataFrame({
        "file_path": file_paths_test.values,
        "replay_id": replay_ids_test.values,
        "true_label": y_test.values,
        "pred_label": y_pred_thresh,
        "prob": y_prob
    })

    # 7. 回放号级别聚合: 如果同一 replay_id 下任意一个段 pred_label=1，则整场为1
    df_file_level = df_results.groupby("replay_id").agg(
        true_label=("true_label", "max"),  # hack 文件下所有段都=1；normal 都=0，因此 max/first 都一样
        pred_label=("pred_label", "max"),  # 只要有一个=1，就=1
        prob=("prob", "max")  # 取最大概率，供参考
    ).reset_index()

    # 8. 打印回放号级别分类报告
    print(f"\n=== File-level Classification Report (Threshold={threshold}) ===")
    print(classification_report(df_file_level["true_label"], df_file_level["pred_label"]))

    # 9. 保存被错误分类的回放号
    misclassified_df = df_file_level[df_file_level["true_label"] != df_file_level["pred_label"]]
    misclassified_path = os.path.join(data_folder_path, "data", "misclassified_files.csv")
    misclassified_df.to_csv(misclassified_path, index=False)
    print(f"Misclassified files saved to: {misclassified_path}")

    # 10. 导出模型
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    model_filename = f"reach_detect_{len(data)}_model_{timestamp}.joblib"
    model_folder = os.path.join(data_folder_path, "model")
    os.makedirs(model_folder, exist_ok=True)
    model_filepath = os.path.join(model_folder, model_filename)
    dump(clf, model_filepath)
    print(f"Model exported to {model_filepath}")


if __name__ == "__main__":
    # 示例调用
    base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    train_reach(base_path, threshold=0.7)
