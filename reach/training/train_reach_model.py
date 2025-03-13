import glob
import os
import re

import pandas as pd
from joblib import dump
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
from sklearn.model_selection import train_test_split

from reach.utils.extract_features import extract_features


def parse_replay_id(filename):
    """
    从文件名中提取回放号
    :param filename: 带有segment的文件名
    :return: 正确的文件名
    """
    base = os.path.basename(filename)
    match = re.match(r"(.*)_segment_\d+\.csv", base)
    if match:
        return match.group(1)
    else:
        return base


def load_segment_dataset(data_folder_path):
    """
    从 processed_csv/hack 和 processed_csv/normal 中加载所有 segment CSV
    每个片段被视为一个样本进行特征提取
    :param data_folder_path: 包含 processed_csv/hack 和 processed_csv/normal 的根目录
    :return: 包含特征、label、file_path、replay_id 的 DataFrame
    """
    data_list = []
    hack_path = os.path.join(data_folder_path, "data", "processed_csv", "hack", "*.csv")
    normal_path = os.path.join(data_folder_path, "data", "processed_csv", "normal", "*.csv")

    hack_files = glob.glob(hack_path)
    normal_files = glob.glob(normal_path)

    print(f"Found {len(hack_files)} hack segment files.")
    print(f"Found {len(normal_files)} normal segment files.")
    # 加载外挂片段
    for file in hack_files:
        feats = extract_features(file)
        if feats is not None and not feats.empty:
            feats['label'] = 1
            feats['file_path'] = file
            feats['replay_id'] = parse_replay_id(file)
            data_list.append(feats)

    # 加载正常片段
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


def train_reach(threshold, misclassified_path,
                data_folder_path=os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))):
    """
    训练模型
    :param misclassified_path: 错误分类的回放号保存路径
    :param threshold: 判断阈值（概率）
    :param data_folder_path: 包含 processed_csv/hack 和 processed_csv/normal 的根目录
    :return: 打印并保存文件
    """

    # 1. 加载数据
    data = load_segment_dataset(data_folder_path)
    print("Number of total segment samples:", len(data))

    # 2. 打标签
    X = data.drop(columns=['label', 'file_path', 'replay_id'])
    y = data['label']
    file_paths = data['file_path']
    replay_ids = data['replay_id']

    # 3. 划分训练集和测试集，保留两成作为测试集
    (X_train, X_test, y_train, y_test,
     file_paths_train, file_paths_test,
     replay_ids_train, replay_ids_test) = train_test_split(
        X, y, file_paths, replay_ids,
        test_size=0.2, random_state=42, stratify=y
    )

    # 4. 训练模型，随机森林
    clf = RandomForestClassifier(n_estimators=100, random_state=42)
    clf.fit(X_train, y_train)

    # 5. 测试集预测
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

    # 7. 回放号级别聚合: 如果同一对局下任意一个段 pred_label=1，则整场为1
    df_file_level = df_results.groupby("replay_id").agg(
        true_label=("true_label", "max"),
        pred_label=("pred_label", "max"),
        prob=("prob", "max")
    ).reset_index()

    # 8. 打印回放号级别分类报告
    print(f"\n=== File-level Classification Report (Threshold={threshold}) ===")
    print(classification_report(df_file_level["true_label"], df_file_level["pred_label"]))

    # 9. 保存被错误分类的回放号
    misclassified_df = df_file_level[df_file_level["true_label"] != df_file_level["pred_label"]]
    misclassified_df.to_csv(misclassified_path, index=False)
    print(f"Misclassified files saved to: {misclassified_path}")

    # 10. 导出模型
    # timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    model_filename = f"reach_detect_{len(data)}_model.joblib"
    model_folder = os.path.join(data_folder_path, "model")
    os.makedirs(model_folder, exist_ok=True)
    model_filepath = os.path.join(model_folder, model_filename)
    dump(clf, model_filepath)
    print(f"Model exported to {model_filepath}")
