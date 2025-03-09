import ast

import numpy as np
import pandas as pd


def extract_features(csv_path):
    """
    读取完整 CSV 并针对整份数据提取统计特征。

    可能的列(数值/JSON)：
        - tick
        - distance
        - train_target_ping, train_target_yaw, train_target_pitch
        - target_player, target_yaw, target_pitch, target_ping

    解析逻辑:
        1. 逐行对可能为 JSON 格式的列进行 safe_extract，取出对应 key 的数值；
        2. 对所有数值列统一填充缺失值为 0；
        3. 计算以下特征:
            - 对每个数值列: min, max, mean, std
            - tick_range = max(tick) - min(tick)
            - tick_count = len(df)

    返回:
        - DataFrame (单行), 每列对应一种特征
    """
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"Failed to read file {csv_path}: {e}")
        return None

    # 可能存在 JSON 格式的列时，定义安全解析函数
    def safe_extract(dictionary_str, key):
        if pd.notnull(dictionary_str):
            try:
                parsed = ast.literal_eval(dictionary_str)
                return parsed.get(key, np.nan)
            except Exception:
                return np.nan
        return np.nan

    # 如果下面这些列可能是 JSON，需要做解析
    # （若实际只有部分列是 JSON，可自行删减）
    json_like_columns = [
        'train_target_ping', 'train_target_yaw', 'train_target_pitch',
        'target_yaw', 'target_pitch', 'target_ping', 'target_player'
    ]
    for col in json_like_columns:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: safe_extract(x, col))

    # 填充缺失值为 0
    numeric_cols = [
        'tick', 'distance',
        'train_target_ping', 'train_target_yaw', 'train_target_pitch',
        'target_player', 'target_yaw', 'target_pitch', 'target_ping'
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].fillna(0)
        else:
            # 若列不存在，也可直接补充 0，避免后续计算报错
            df[col] = 0

    # 定义一个统计量计算的辅助函数
    def compute_stats(series, prefix):
        return {
            f"{prefix}_min": series.min(),
            f"{prefix}_max": series.max(),
            f"{prefix}_mean": series.mean(),
            f"{prefix}_std": series.std()
        }

    # 如果表是空表，直接返回一个空特征行
    if len(df) == 0:
        return pd.DataFrame([{
            'tick_range': 0,
            'tick_count': 0,
            **{f"{col}_{stat}": 0 for col in numeric_cols for stat in ['min', 'max', 'mean', 'std']}
        }])

    # 先计算一些全局特征
    features = {
        'tick_range': df['tick'].max() - df['tick'].min(),
        'tick_count': len(df),
    }

    # 对每个数值列，计算 min/max/mean/std
    for col in numeric_cols:
        stats_dict = compute_stats(df[col], prefix=col)
        features.update(stats_dict)

    # 返回单行 DataFrame
    return pd.DataFrame([features])


# 测试示例：
if __name__ == "__main__":
    csv_file = "path_to_your_full_file.csv"
    features_df = extract_features(csv_file)
    print(features_df)
