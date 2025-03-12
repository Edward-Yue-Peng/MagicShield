import os

import pandas as pd
from joblib import load

from reach.utils.extract_features import extract_features


def extract_segments_from_csv(csv_path, min_ticks, distance_threshold=3):
    df = pd.read_csv(csv_path)
    df = df.sort_values(by='tick')
    segments = []
    current_segment = []
    for _, row in df.iterrows():
        if row['distance'] > distance_threshold:
            current_segment.append(row)
        else:
            if len(current_segment) >= min_ticks:
                segments.append(pd.DataFrame(current_segment))
            current_segment = []
    if len(current_segment) >= min_ticks:
        segments.append(pd.DataFrame(current_segment))
    return segments


def predict_reach_module(model_path, input_csv, threshold, min_ticks, distance_threshold=3):
    # 1. 加载模型
    clf = load(model_path)

    # 2. 对输入 CSV 切分
    segments = extract_segments_from_csv(
        input_csv,
        distance_threshold=distance_threshold,
        min_ticks=min_ticks
    )
    print(segments)
    if not segments:
        print("No valid segments found in this CSV. Default to 'normal' or skip.")
        return 0

    # 3. 对每个段做特征提取，并预测
    is_hack = False
    for i, seg_df in enumerate(segments, start=1):
        temp_csv_path = "temp_segment.csv"
        seg_df.to_csv(temp_csv_path, index=False)
        feats = extract_features(temp_csv_path)
        if feats is None or feats.empty:
            continue  # 跳过无效段
        y_prob = clf.predict_proba(feats)[:, 1]
        print(f"Segment {i} predicted probabilities: {y_prob}")
        y_pred_thresh = (y_prob >= threshold).astype(int)
        if (y_pred_thresh == 1).any():
            # 只要其中一个片段判断为hack，整个对局就算hack
            is_hack = True
            print(f"Segment {i} predicted as hack (prob={y_prob.max():.3f})")
            break

    # 4. 最终判断
    if is_hack:
        print("Final Prediction: HACK")
        return 1
    else:
        print("Final Prediction: NORMAL")
        return 0


def predict_reach(predict_threshold, predict_min_ticks):
    """
    预测original_csv/test下的文件是开挂还是正常
    :param predict_threshold: 用于预测的阈值
    :param predict_min_ticks: 用于触发预测的最小连续超过攻击距离3的tick数
    :return 直接打印结果
    """
    base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    model_file = os.path.join(base_path, "model", "reach_detect_1494_model.joblib")
    hack_count = 0
    file_count = 0
    predict_path = os.path.join(base_path, "data", "original_csv", "test")
    for root, dirs, files in os.walk(predict_path):
        for file in files:
            if file.endswith(".csv"):
                file_count += 1
                hack_count += predict_reach_module(model_file, os.path.join(root, file), threshold=predict_threshold,
                                                   distance_threshold=3, min_ticks=predict_min_ticks)
    print(f"Hack Count: {hack_count}, Total Files: {file_count}")
