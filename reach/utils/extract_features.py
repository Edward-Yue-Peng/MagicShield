import pandas as pd


def extract_features(csv_path):
    """
    读取完整 CSV 并针对整份数据提取统计特征。

    处理的列：
        - tick, distance, train_target_ping, train_target_yaw, train_target_pitch,
          target_yaw, target_pitch, target_ping 作为数值列计算 min, max, mean, std
        - tick_range = max(tick) - min(tick)
        - tick_count = 数据行数
        - target_player 直接取第1行的值（假定该列为字符串）

    返回:
        - DataFrame (单行), 每列对应一种特征
    """
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"Failed to read file {csv_path}: {e}")
        return None

    # 数值列列表（不包括 target_player）
    numeric_cols = [
        'tick', 'distance', 'train_target_ping', 'train_target_yaw', 'train_target_pitch',
        'target_yaw', 'target_pitch', 'target_ping', 'relative_speed', 'train_target_speed', 'target_speed'
    ]

    # 确保数值列为数字，填充缺失值为 0
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        else:
            df[col] = 0

    # 计算全局特征
    features = {
        # 'relative_speed_max': df['relative_speed'].max(),
        # 'relative_speed_mean': df['relative_speed'].mean(),
        # 'relative_speed_std': df['relative_speed'].std(),
        # 'train_target_speed_mean': df['train_target_speed'].mean(),
        # 'target_speed_mean': df['target_speed'].mean(),
        'distance_max': df['distance'].max(),
        'distance_mean': df['distance'].mean(),
        'distance_std': df['distance'].std(),
        'tick': (df['tick'].max() - df['tick'].min()) / df['tick'].count(),
        'train_target_ping_std': df['train_target_ping'].std(),
        'target_ping_std': df['target_ping'].std(),
        'train_target_ping_max': df['train_target_ping'].max(),
        'target_ping_max': df['target_ping'].max(),
        'train_target_yaw_std': df['train_target_yaw'].std(),
        'target_yaw_std': df['target_yaw'].std(),
        'target_pitch_std': df['target_pitch'].std(),
        'train_target_pitch_std': df['target_pitch'].std(),
    }
    # 对每个数值列计算统计特征
    # print(features)
    return pd.DataFrame([features])


# 测试示例：
if __name__ == "__main__":
    csv_file = "/Users/pengyue/Documents/GitHub/MagicShield/reach/data/processed_csv/hack/20809055_segment_1.csv"
    features_df = extract_features(csv_file)
    print(features_df)
