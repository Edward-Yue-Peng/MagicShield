import os

import pandas as pd


def extract_high_distance_segments_recursive(input_folder_path, output_folder_path,
                                             distance_threshold=3, min_ticks=10):
    """
    递归遍历 input_folder_path 下的所有子文件夹，处理每个 CSV 文件，
    将符合条件的段切分并统一输出到 output_folder_path 下。
    input_folder_path: 输入文件夹路径
    output_folder_path: 输出文件夹路径
    distance_threshold: 距离阈值
    min_ticks: 最小段长度
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


if __name__ == "__main__":
    base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    # hack 和 normal 一起处理
    for subdir in ["hack", "normal"]:
        input_folder = os.path.join(base_path, "data", "original_csv", subdir)
        output_folder = os.path.join(base_path, "data", "processed_csv", subdir)
        print(f"Start processing: {subdir}")
        extract_high_distance_segments_recursive(
            input_folder_path=input_folder,
            output_folder_path=output_folder,
            distance_threshold=3,
            min_ticks=8
        )
