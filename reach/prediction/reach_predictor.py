import os

import pandas as pd
from joblib import load

# 如果你有自己的特征提取函数
from reach.utils.extract_features import extract_features


def extract_segments_from_csv(csv_path, distance_threshold=3, min_ticks=8):
    """
    根据你给出的逻辑，对单个 CSV 做切分，返回若干符合条件(连续10tick且distance>3)的段 DataFrame 列表。
    """
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

    # 最后再检查一次
    if len(current_segment) >= min_ticks:
        segments.append(pd.DataFrame(current_segment))

    return segments


def predict_reach(model_path, input_csv, threshold=0.75, distance_threshold=3, min_ticks=8):
    """
    使用训练好的模型，对单个「完整」CSV 进行切分 -> 特征提取 -> 预测。
    只要任意一个段被判定为作弊，则最终结果为作弊。
    """
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
        # 如果没有切到任何符合条件的段，根据业务需求自行处理
        print("No valid segments found in this CSV. Default to 'normal' or skip.")
        return 0  # 或者返回 None

    # 3. 对每个段做特征提取，并预测
    is_hack = False
    for i, seg_df in enumerate(segments, start=1):
        # 先把这个段导出到临时CSV，或者直接在内存中处理
        # 这里示例使用 extract_features_in_chunks，如果你只想把 seg_df 视为一个chunk，可存到临时文件：
        temp_csv_path = "temp_segment.csv"
        seg_df.to_csv(temp_csv_path, index=False)

        feats = extract_features(temp_csv_path)
        if feats is None or feats.empty:
            continue  # 跳过无效段
        # 可能 feats 还会返回多行，这里假设只返回 1 行
        # 如果返回多行，你可自行聚合或只取第一行
        # 这里简单取全部行做预测，然后看是否有任意行判定为 hack
        y_prob = clf.predict_proba(feats)[:, 1]
        print(f"Segment {i} predicted probabilities: {y_prob}")
        y_pred_thresh = (y_prob >= threshold).astype(int)
        if (y_pred_thresh == 1).any():
            # 只要其中一个行判断为hack，整段就算hack
            is_hack = True
            print(f"Segment {i} predicted as hack (prob={y_prob.max():.3f})")
            break

    # 4. 最终回放级别判断
    if is_hack:
        print("Final Prediction: HACK")
        return 1
    else:
        print("Final Prediction: NORMAL")
        return 0


if __name__ == "__main__":
    # 示例调用
    base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    model_file = os.path.join(base_path, "model", "reach_detect_1494_model.joblib")
    test_csv = "/Users/pengyue/Documents/GitHub/MagicShield/reach/data/original_csv/test/21196695.csv"
    # test_csv = os.path.join(base_path, "data", "original_csv", "hack", "netease7e491a09", "20810041.csv")  # 一个完整回放
    # predict_reach(model_file, test_csv, threshold=0.7, distance_threshold=3)
    hack_count = 0
    file_count = 0
    for root, dirs, files in os.walk("/Users/pengyue/Documents/GitHub/MagicShield/reach/data/original_csv/test"):
        # file_count += len(files)
        for file in files:
            if file.endswith(".csv"):
                file_count += 1
                hack_count += predict_reach(model_file, os.path.join(root, file), threshold=0.7, distance_threshold=3)
    print(f"Hack Count: {hack_count}, Total Files: {file_count}")
