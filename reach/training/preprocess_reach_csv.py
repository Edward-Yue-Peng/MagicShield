import os

import pandas as pd


def extract_high_distance_segments_recursive(input_folder_path, output_folder_path,
                                             distance_threshold, min_ticks):
    """
    递归遍历 input_folder_path 下所有文件，提取每个文件中的异常攻击距离片段
    :param input_folder_path: 输入文件夹路径（original）
    :param output_folder_path: 输出文件夹路径（processed）
    :param distance_threshold: 攻击阈值（应该为3）
    :param min_ticks: 最小连续超过攻击距离3的tick数
    :return: 直接处理文件，输出到 output_folder_path
    """
    if not os.path.exists(output_folder_path):
        os.makedirs(output_folder_path)
    file_count = 0
    segment_count = 0
    # 递归遍历 input_folder_path 下所有文件
    for root, dirs, files in os.walk(input_folder_path):
        for filename in files:
            if filename.endswith(".csv"):
                file_count += 1
                file_path = os.path.join(root, filename)
                df = pd.read_csv(file_path)
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

                # 将每个符合条件的段输出到同一个 output_folder_path
                for i, segment in enumerate(segments):
                    # 拼接输出文件名
                    output_filename = f"{os.path.splitext(filename)[0]}_segment_{i + 1}.csv"
                    output_path = os.path.join(output_folder_path, output_filename)
                    segment.to_csv(output_path, index=False)
                    segment_count += 1

    print(f"Found {file_count} CSV files.")
    print(f"Processed {segment_count} segments in total.")


def preprocess_reach_csv(min_ticks_per_segment):
    """
    预处理所有原始 CSV 文件，提取高攻击距离片段
    :param min_ticks_per_segment: 最小连续超过攻击距离3的tick数
    :return: 直接操作文件
    """
    base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    # hack 和 normal 分别处理
    for subdir in ["hack", "normal"]:
        input_folder = os.path.join(base_path, "data", "original_csv", subdir)
        output_folder = os.path.join(base_path, "data", "processed_csv", subdir)
        print(f"Start processing: {subdir}")
        extract_high_distance_segments_recursive(
            input_folder_path=input_folder,
            output_folder_path=output_folder,
            distance_threshold=3,
            min_ticks=min_ticks_per_segment
        )
