import ast
import pandas as pd
import numpy as np

def extract_features(csv_path):
    """
    读取 CSV 文件并提取特征：
    - 计算 ping、yaw、pitch、distance 的统计信息
    - 计算对局 tick 数
    """
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"Failed to read file {csv_path}: {e}")
        return None

    # 解析 Ping 和 Rotation 数据
    df['ping'] = df['Ping'].apply(lambda x: ast.literal_eval(x)['ping'] if pd.notnull(x) else np.nan)
    df['yaw'] = df['Rotation'].apply(lambda x: ast.literal_eval(x)['yaw'] if pd.notnull(x) else np.nan)
    df['pitch'] = df['Rotation'].apply(lambda x: ast.literal_eval(x)['pitch'] if pd.notnull(x) else np.nan)

    # 处理缺失值
    df[['Distance', 'ping', 'yaw', 'pitch']] = df[['Distance', 'ping', 'yaw', 'pitch']].fillna(0)

    # 计算统计特征
    features = {f'{col}_{stat}': getattr(df[col], stat)() for col in ['Distance', 'ping', 'yaw', 'pitch']
                for stat in ['mean', 'std', 'min', 'max']}

    # 记录对局 tick 数
    features['num_ticks'] = len(df)

    return features
