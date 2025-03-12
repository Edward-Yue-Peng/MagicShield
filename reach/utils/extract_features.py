import pandas as pd


def extract_features(csv_path):
    """
    读取完整 CSV 并针对整份数据提取统计特征。
    :param csv_path: csv 文件路径
    :return: 包含特征的 DataFrame
    """
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"Failed to read file {csv_path}: {e}")
        return None

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

    # 提取特征
    features = {
        # 'relative_speed_max': df['relative_speed'].max(),
        'relative_speed_mean': df['relative_speed'].mean(),
        'relative_speed_std': df['relative_speed'].std(),
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
    return pd.DataFrame([features])
