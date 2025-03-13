import csv
import csv
import os

import pandas as pd
from joblib import load

from reach.utils.convert_csv import avro_reader, metadata_reader, pair_entity_id, process_attack_events, \
    write_attack_events
from reach.utils.extract_features import extract_features


def extract_segments_from_csv(csv_path, min_ticks, distance_threshold=3):
    """
    从 CSV 文件中提取连续 tick 数大于 min_ticks 的片段，返回一个 DataFrame 列表。
    :param csv_path: csv文件路径
    :param min_ticks: 最小连续异常攻击距离数
    :param distance_threshold: 异常攻击距离阈值
    :return: DataFrame 列表
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
    if len(current_segment) >= min_ticks:
        segments.append(pd.DataFrame(current_segment))
    return segments


def predict_reach_module(model_path, input_csv, threshold, min_ticks, distance_threshold=3):
    """
    对单个对局进行预测，检测到可疑开挂就返回1，否则返回0
    :param model_path: 模型路径
    :param input_csv: 对局CSV文件
    :param threshold: 判断阈值
    :param min_ticks: 最小连续异常攻击距离数
    :param distance_threshold: 攻击距离阈值
    :return: 开挂返回1，正常返回0
    """
    clf = load(model_path)
    segments = extract_segments_from_csv(input_csv, min_ticks, distance_threshold)
    print(f"Processing {input_csv}, found segments: {len(segments)}")
    if not segments:
        print(f"No valid segments found in {input_csv}, default to NORMAL")
        return 0

    is_hack = False
    for i, seg_df in enumerate(segments, start=1):
        temp_csv_path = "temp_segment.csv"
        seg_df.to_csv(temp_csv_path, index=False)
        feats = extract_features(temp_csv_path)
        if feats is None or feats.empty:
            continue
        y_prob = clf.predict_proba(feats)[:, 1]
        print(f"The probability of segment {i} in {input_csv}: {y_prob}")
        y_pred_thresh = (y_prob >= threshold).astype(int)
        if (y_pred_thresh == 1).any():
            is_hack = True
            print(f"[{input_csv}] Segment {i} judged as hack (max prob={y_prob.max():.3f})")
            break

    if is_hack:
        print(f"Final prediction for {input_csv}: HACK")
        return 1
    else:
        print(f"Final prediction for {input_csv}: NORMAL")
        return 0


def predict_reach(predict_path, model_file, predict_threshold, predict_min_ticks):
    """
    针对单个玩家的多局对局进行预测，遍历 data/original_csv/test 下的所有 CSV 文件，
    并统计判断为 hack 的局数（适用于对单个玩家的分析判断）。
    :param predict_path: 要针对单个玩家的分析的csv文件目录
    :param model_file: 模型路径
    :param predict_threshold: 判断阈值
    :param predict_min_ticks: 最小连续异常攻击距离数
    :return: 直接打印贩毒案结果
    """
    hack_count = 0
    file_count = 0
    for root, dirs, files in os.walk(predict_path):
        for file in files:
            if file.endswith(".csv"):
                file_count += 1
                csv_file = os.path.join(root, file)
                hack_count += predict_reach_module(model_file, csv_file,
                                                   threshold=predict_threshold,
                                                   min_ticks=predict_min_ticks,
                                                   distance_threshold=3)
    print(f"Hack Count: {hack_count}, Total Files: {file_count}")


def predict_with_tick_range(model_path, input_csv, threshold, min_ticks, distance_threshold=3):
    """
    对单个 CSV 文件进行预测，若检测到 hack，则返回 (True, (min_tick, max_tick))
    :param model_path: 模型路径
    :param input_csv: 判断csv
    :param threshold: 判断阈值
    :param min_ticks: 最小连续异常攻击距离数
    :param distance_threshold: 异常攻击距离阈值
    :return: (判断结果，可疑片段的 tick 范围)
    """
    clf = load(model_path)
    segments = extract_segments_from_csv(input_csv, min_ticks, distance_threshold)
    if not segments:
        print(f"{input_csv}: No valid segments found")
        return False, None

    for i, seg_df in enumerate(segments, start=1):
        temp_csv_path = "temp_segment.csv"
        seg_df.to_csv(temp_csv_path, index=False)
        feats = extract_features(temp_csv_path)
        if feats is None or feats.empty:
            continue  # 跳过无效段
        y_prob = clf.predict_proba(feats)[:, 1]
        print(f"The probability of segment {i} in {input_csv}: {y_prob}")
        y_pred_thresh = (y_prob >= threshold).astype(int)
        if (y_pred_thresh == 1).any():
            min_tick = seg_df['tick'].min()
            max_tick = seg_df['tick'].max()
            print(f"Final prediction for {input_csv}: HACK, segment {i} (tick range: {min_tick}-{max_tick})")
            return True, (min_tick, max_tick)
    return False, None


def process_predict_replay_file(avro_dir, filename, output_base_dir, replay_game_dict):
    """
    对单个回放文件进行处理，生成该回放下所有玩家的攻击数据 CSV。
    输出路径：output_base_dir/{replay_id}/{player_ecid}.csv
    :param avro_dir: avro 文件目录
    :param filename: 处理的文件名称
    :param output_base_dir: 输出的 CSV 文件目录（original）
    :return: 直接操作文件
    """
    replay_id = filename[:-5]  # 去掉 .avro 后缀
    avro_filepath = os.path.join(avro_dir, filename)
    schema_filepath = os.path.join(avro_dir, replay_id + ".avsc")
    metadata_filepath = os.path.join(avro_dir, replay_id + ".metadata.json")

    if not os.path.exists(schema_filepath) or not os.path.exists(metadata_filepath):
        print(f"Lack schema or metadata file for {replay_id}, skipping...")
        return

    # 读取 avro 数据和 metadata
    avro_data = avro_reader(avro_filepath, schema_filepath)
    metadata = metadata_reader(metadata_filepath)
    pair_dict = pair_entity_id(metadata)
    game_type = metadata.get("game", "")
    replay_game_dict[replay_id] = game_type
    players = metadata.get("players", [])
    processed_ecids = set()
    for player in players:
        # 这里使用 name 字段作为玩家标识（ecid）
        train_target = player["name"]
        # 如果已经处理过该玩家ecid，就直接跳过
        if train_target in processed_ecids:
            continue
        processed_ecids.add(train_target)
        records = process_attack_events(avro_data, train_target, pair_dict)
        output_replay_dir = os.path.join(output_base_dir, replay_id)
        if not os.path.exists(output_replay_dir):
            os.makedirs(output_replay_dir)
        output_csv = os.path.join(output_replay_dir, f"{train_target}.csv")
        success = write_attack_events(records, output_csv)
        if success:
            print(f"Generated attack data: {output_csv}")


def predict_hack_from_csv_files(model_path, csv_base_dir, threshold, min_ticks, replay_game_dict):
    """
    遍历 csv_base_dir 下的每个回放目录，对其中每个玩家 CSV 调用预测函数。
    :param model_path: 模型路径
    :param csv_base_dir: csv 文件目录
    :param threshold: 判断阈值
    :param min_ticks: 最小连续异常攻击距离数
    :param replay_game_dict: 回放号到游戏类型 的映射
    :return: 一条数据，包含ecid，回放号，可以片段
    """
    results = []
    for replay_id in os.listdir(csv_base_dir):
        replay_dir = os.path.join(csv_base_dir, replay_id)
        if not os.path.isdir(replay_dir):
            continue
        for csv_file in os.listdir(replay_dir):
            if not csv_file.endswith(".csv"):
                continue
            player_ecid = csv_file[:-4]
            csv_path = os.path.join(replay_dir, csv_file)
            is_hack, tick_range = predict_with_tick_range(model_path, csv_path, threshold, min_ticks,
                                                          distance_threshold=3)
            if is_hack:
                tick_range_str = f"{tick_range[0]}-{tick_range[1]}"
                results.append({
                    "ecid": player_ecid,
                    "replay_id": replay_id,
                    "tick_range": tick_range_str,
                    "game": replay_game_dict.get(replay_id, "")
                })
    return results


def write_suspected_hacks(results, output_file):
    """
    将疑似 hack 的结果写入 CSV 文件。
    :param results: 列表
    :param output_file: csv输出路径
    :return: 直接操作文件
    """
    header = ["ecid", "replay_id", "tick_range", "game"]
    with open(output_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(results)
    print(f"Suspected hack results written to: {output_file}")


def predict_reach_large_scale(avro_predict_dir, output_csv_dir, predict_report_dir, model_path, predict_threshold,
                              predict_min_ticks):
    """
    运行完整的多回放预测流程：
      1. 解码 avro 文件，为每个回放中所有玩家生成攻击数据 CSV
      2. 遍历生成的 CSV 文件，判断每个玩家的每个对局
      3. 将疑似开挂的结果（玩家 ecid、回放号、可疑片段 tick 范围）写入 CSV 文件
    :param predict_report_dir: 保存预测结果的csv文件路径
    :param output_csv_dir: 提取攻击距离的输出csv文件路径
    :param avro_predict_dir: avro文件
    :param model_path: 模型路径
    :param predict_threshold: 判断阈值
    :param predict_min_ticks: 最小连续异常攻击距离数
    :return: 操作文件
    """
    # 保存回放号和游戏类型的映射
    replay_game_dict = {}

    # 1. 处理每个 avro 文件
    for filename in os.listdir(avro_predict_dir):
        if filename.endswith(".avro"):
            process_predict_replay_file(avro_predict_dir, filename, output_csv_dir, replay_game_dict)

    # 2. 遍历生成的 CSV 文件并进行预测
    results = predict_hack_from_csv_files(model_path, output_csv_dir, predict_threshold, predict_min_ticks,
                                          replay_game_dict)

    # 3. 写入疑似 hack 的结果到 CSV 文件
    write_suspected_hacks(results, predict_report_dir)
