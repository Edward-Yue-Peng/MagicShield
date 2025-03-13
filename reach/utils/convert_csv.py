import csv
import json
import math
import os

from fastavro import schemaless_reader


def process_attack_events(data_dict, train_target_ecid, pair_dict):
    """
    提取训练长臂需要的信息，传入训练目标（也就是要提取谁的信息），返回攻击事件的相关信息。
    根据原始的数据计算速度、速度向量、相对速度和距离。
    :param data_dict: avro文件（已经解析为字典）
    :param train_target_ecid: 训练目标
    :param pair_dict: 玩家名与entityID配对的字典
    :return: 一个包含攻击事件信息的列表
    """
    ticks = data_dict.get("ticks", [])
    results = []
    # prev_known 存储上一个 tick 的玩家状态，包括位置、旋转、ping、速度和速度向量
    prev_known = {}

    # 遍历每个 tick
    for tick_entry in ticks:
        tick = tick_entry.get("tick")
        players_data = tick_entry.get("data", {}).get("players", {})
        # 从上一个 tick 状态开始构造当前 tick 状态
        current_known = {player: info.copy() for player, info in prev_known.items()}
        attack_events = []

        # 遍历本 tick 中所有玩家的事件，更新状态
        for player, events in players_data.items():
            for event in events:
                event_type = event.get("type", "")
                updated = event.get("updated", {})

                if event_type == "PlayerUpdatedPositionXYZ":
                    new_pos = (updated.get("x"), updated.get("y"), updated.get("z"))
                    # 若有上一个 tick 的位置，则计算速度向量与瞬时速度
                    if player in current_known and "pos" in current_known[player]:
                        old_pos = current_known[player]["pos"]
                        if None not in old_pos and None not in new_pos:
                            # 计算坐标差，并乘以20得到速度向量
                            vel = ((new_pos[0] - old_pos[0]) * 20,
                                   (new_pos[1] - old_pos[1]) * 20,
                                   (new_pos[2] - old_pos[2]) * 20)
                            speed = math.sqrt(vel[0] ** 2 + vel[1] ** 2 + vel[2] ** 2)
                        else:
                            vel = None
                            speed = ""
                    else:
                        vel = None
                        speed = ""
                    current_known.setdefault(player, {})
                    current_known[player]["pos"] = new_pos
                    current_known[player]["vel"] = vel
                    current_known[player]["speed"] = speed

                if "yaw" in updated and "pitch" in updated:
                    current_known.setdefault(player, {})
                    current_known[player]["rot"] = (updated["yaw"], updated["pitch"])

                if event_type == "PlayerUpdatedPing":
                    current_known.setdefault(player, {})
                    current_known[player]["ping"] = updated.get("ping", "")

                # 收集训练目标的攻击事件
                if player == train_target_ecid and "attackTarget" in updated:
                    attack_events.append(event)

        # 若本 tick 中训练目标存在攻击事件，才开始记录
        if attack_events:
            for event in attack_events:
                updated = event.get("updated", {})
                target_entity = updated["attackTarget"]
                target_player = pair_dict.get(target_entity)

                attacker_data = current_known.get(train_target_ecid, {})
                target_data = current_known.get(target_player, {})

                attacker_pos = attacker_data.get("pos")
                attacker_rot = attacker_data.get("rot")
                attacker_ping = attacker_data.get("ping", "")
                attacker_speed = attacker_data.get("speed", "")
                attacker_vel = attacker_data.get("vel")

                target_pos = target_data.get("pos")
                target_rot = target_data.get("rot")
                target_ping = target_data.get("ping", "")
                target_speed = target_data.get("speed", "")
                target_vel = target_data.get("vel")

                # 计算两者之间的距离
                if attacker_pos is not None and target_pos is not None:
                    distance = math.sqrt(
                        (attacker_pos[0] - target_pos[0]) ** 2 +
                        (attacker_pos[1] - target_pos[1]) ** 2 +
                        (attacker_pos[2] - target_pos[2]) ** 2
                    )
                else:
                    distance = ""

                # 计算相对速度
                if attacker_vel is not None and target_vel is not None:
                    rel_vel = (attacker_vel[0] - target_vel[0],
                               attacker_vel[1] - target_vel[1],
                               attacker_vel[2] - target_vel[2])
                    relative_speed = math.sqrt(rel_vel[0] ** 2 + rel_vel[1] ** 2 + rel_vel[2] ** 2)
                else:
                    relative_speed = ""

                # 写入
                record = {
                    "tick": tick,
                    "train_target_yaw": attacker_rot[0] if attacker_rot else "",
                    "train_target_pitch": attacker_rot[1] if attacker_rot else "",
                    "train_target_ping": attacker_ping,
                    "train_target_speed": attacker_speed,
                    "target_player": target_player,
                    "target_yaw": target_rot[0] if target_rot else "",
                    "target_pitch": target_rot[1] if target_rot else "",
                    "target_ping": target_ping,
                    "target_speed": target_speed,
                    "relative_speed": relative_speed,
                    "distance": distance
                }
                results.append(record)
        # 当前 tick 完成后，将状态保存作为下一 tick 的初始状态（因为回放只记录更新记录而不是持续状态）
        prev_known = {player: info.copy() for player, info in current_known.items()}
    return results


def write_attack_events(records, csv_filepath):
    """
    将攻击事件写入 csv 文件
    :param records: 攻击事件的列表
    :param csv_filepath: 输出的文件路径
    :return:
    """
    if not records:
        print(f"{csv_filepath}: No attack event data found!")
        return False

    header = [
        "tick", "distance", "train_target_ping",
        "train_target_yaw", "train_target_pitch", "train_target_speed",
        "target_player", "target_yaw", "target_pitch", "target_ping", "target_speed",
        "relative_speed",
    ]

    with open(csv_filepath, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(records)
    return True


def avro_reader(avro_filepath, schema_filepath):
    """
    读取 avro 文件
    :param avro_filepath: avro 文件路径
    :param schema_filepath: schema 文件路径（avsc文件）
    :return: 解码后的 avro 数据
    """
    with open(schema_filepath, "r", encoding="utf-8") as schema_file:
        schema = json.load(schema_file)
    with open(avro_filepath, "rb") as f:
        avro_output = schemaless_reader(f, schema)
    return avro_output


def metadata_reader(metadata_filepath):
    """
    读取 metadata 文件
    :param metadata_filepath: metadata 文件路径（json文件）
    :return: 读取后的json文件
    """
    with open(metadata_filepath, "r", encoding="utf-8") as f:
        metadata = json.load(f)
    return metadata


def pair_entity_id(metadata_dict):
    """
    从 metadata 中提取玩家名与 entityID 的映射关系的字典
    :param metadata_dict: metadata 文件
    :return: entityID 与玩家名的映射字典
    """
    pair_dict = {}
    for player in metadata_dict.get("players", []):
        pair_dict[player["entityID"]] = player["name"]
    return pair_dict


def process_replay_files(avro_dir, output_dir, train_target):
    """
    处理给定目录下的所有 .avro 文件，并将生成的 CSV 写入 output_dir
    :param avro_dir: avro 文件目录
    :param output_dir: 输出的 CSV 文件目录（original）
    :param train_target: 训练目标
    :return: 直接操作文件
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


def convert_csv_for_training(base_avro_dir, base_output_dir):
    # 遍历 normal 和 hack 两个目录
    for subdir in ["normal", "hack"]:
        subdir_path = os.path.join(base_avro_dir, subdir)
        output_subdir = os.path.join(base_output_dir, subdir)
        # 遍历 normal/hack 下的所有子文件夹（以 ecid 命名）
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
