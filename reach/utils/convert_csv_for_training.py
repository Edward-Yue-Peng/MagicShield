import csv
import json
import math
import os

from fastavro import schemaless_reader


def process_attack_events(data_dict, train_target_ecid, pair_dict):
    """
    提取训练reach需要的信息，传入train_target，返回攻击事件的相关信息。
    """
    ticks = data_dict.get("ticks", [])
    last_known = {}
    results = []
    # 遍历每一个tick
    for tick_entry in ticks:
        tick = tick_entry.get("tick")
        players_data = tick_entry.get("data", {}).get("players", {})

        for player, events in players_data.items():
            for event in events:
                event_type = event.get("type", "")
                updated = event.get("updated", {})

                if event_type == "PlayerUpdatedPositionXYZ":
                    last_known.setdefault(player, {})
                    last_known[player]["pos"] = (
                        updated.get("x"), updated.get("y"), updated.get("z")
                    )

                if "yaw" in updated and "pitch" in updated:
                    last_known.setdefault(player, {})
                    last_known[player]["rot"] = (
                        updated["yaw"], updated["pitch"]
                    )

                if event_type == "PlayerUpdatedPing":
                    last_known.setdefault(player, {})
                    last_known[player]["ping"] = updated.get("ping", "")
        # 提取train_target的攻击事件
        if train_target_ecid in players_data:
            for event in players_data[train_target_ecid]:
                updated = event.get("updated", {})
                if "attackTarget" in updated:
                    target_entity = updated["attackTarget"]
                    target_player = pair_dict.get(target_entity)
                    attacker_data = last_known.get(train_target_ecid, {})
                    attacker_pos = attacker_data.get("pos")
                    attacker_rot = attacker_data.get("rot")
                    attacker_ping = attacker_data.get("ping", "")
                    target_data = last_known.get(target_player, {})
                    target_pos = target_data.get("pos")
                    target_rot = target_data.get("rot")
                    target_ping = target_data.get("ping", "")

                    if attacker_pos is not None and target_pos is not None:
                        distance = math.sqrt(
                            (attacker_pos[0] - target_pos[0]) ** 2 +
                            (attacker_pos[1] - target_pos[1]) ** 2 +
                            (attacker_pos[2] - target_pos[2]) ** 2
                        )
                    else:
                        distance = ""

                    record = {
                        "tick": tick,
                        "train_target_yaw": attacker_rot[0] if attacker_rot else "",
                        "train_target_pitch": attacker_rot[1] if attacker_rot else "",
                        "train_target_ping": attacker_ping,
                        "target_player": target_player,
                        "target_yaw": target_rot[0] if target_rot else "",
                        "target_pitch": target_rot[1] if target_rot else "",
                        "target_ping": target_ping,
                        "distance": distance
                    }
                    results.append(record)
                    break
    return results


def write_attack_events(records, csv_filepath):
    """
    将attack事件写入csv
    """
    if not records:
        print(f"{csv_filepath}: No attack event data found!")
        return False

    header = [
        "tick", "distance", "train_target_ping",
        "train_target_yaw", "train_target_pitch",
        "target_player", "target_yaw", "target_pitch", "target_ping",
    ]

    with open(csv_filepath, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(records)
    return True


def avro_reader(avro_filepath, schema_filepath):
    """
    打开avro和avsc文件
    """
    with open(schema_filepath, "r", encoding="utf-8") as schema_file:
        schema = json.load(schema_file)
    with open(avro_filepath, "rb") as f:
        avro_output = schemaless_reader(f, schema)
    return avro_output


def metadata_reader(metadata_filepath):
    """
    读取metadata
    """
    with open(metadata_filepath, "r", encoding="utf-8") as f:
        metadata = json.load(f)
    return metadata


def pair_entity_id(metadata_dict):
    """
    根据 metadata 中的玩家信息，生成 entityID 与玩家名的映射字典。
    """
    pair_dict = {}
    for player in metadata_dict.get("players", []):
        pair_dict[player["entityID"]] = player["name"]
    return pair_dict


def process_replay_files(avro_dir, output_dir, train_target):
    """
    处理给定目录下的所有 .avro 文件，
    并将生成的 CSV 写入 output_dir
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    lack_schema = 0
    success = 0
    no_attack_data = 0

    for filename in os.listdir(avro_dir):
        if filename.endswith(".avro"):
            # 获得回放号
            replay_id = filename[:-5]
            avro_filepath = os.path.join(avro_dir, filename)
            schema_filepath = os.path.join(avro_dir, replay_id + ".avsc")
            metadata_filepath = os.path.join(avro_dir, replay_id + ".metadata.json")
            csv_filepath = os.path.join(output_dir, replay_id + ".csv")

            if not os.path.exists(schema_filepath) or not os.path.exists(metadata_filepath):
                print(f"Lack schema or metadata file for {replay_id}, skipping...")
                lack_schema += 1
                continue
            # 一套小连招
            avro_data = avro_reader(avro_filepath, schema_filepath)
            metadata = metadata_reader(metadata_filepath)
            pair_dict = pair_entity_id(metadata)
            records = process_attack_events(avro_data, train_target, pair_dict)
            if write_attack_events(records, csv_filepath):
                success += 1
            else:
                no_attack_data += 1
    print("Lack of schema or metadata files:", lack_schema)
    print("Successfully processed files:", success)
    print("Files without attack data:", no_attack_data)


if __name__ == "__main__":
    base_avro_dir = "../data/avro_data"  # 这是playback-process的小工具导出的文件
    base_output_dir = "../data/original_csv"  # 输出的原始csv

    # 遍历 normal 和 hack 两个目录
    for subdir in ["normal", "hack"]:
        subdir_path = os.path.join(base_avro_dir, subdir)
        output_subdir = os.path.join(base_output_dir, subdir)
        # 遍历 normal/hack 下的所有子文件夹（以ecid命名）
        for folder in os.listdir(subdir_path):
            folder_path = os.path.join(subdir_path, folder)
            # 确保是子文件夹
            if os.path.isdir(folder_path):
                # 将该子文件夹的名字作为 train_target
                train_target = folder
                # 对应输出目录
                output_folder = os.path.join(output_subdir, folder)
                print(f"Start processing: {folder_path}")
                process_replay_files(folder_path, output_folder, train_target)
