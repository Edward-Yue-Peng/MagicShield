import csv
import json
import math
import os

from fastavro import schemaless_reader


def process_attack_events(data_dict, player_ecid, pair_dict):
    """
    提取攻击事件信息，player_ecid 为当前要提取的玩家 (即原先的 train_target)。
    只要 data_dict 里出现该玩家的攻击事件，就记录相关信息。
    返回一系列记录，每个记录包含：
        tick, distance, train_target_ping, train_target_yaw, train_target_pitch,
        target_player, target_yaw, target_pitch, target_ping
    """
    ticks = data_dict.get("ticks", [])
    last_known = {}
    results = []

    for tick_entry in ticks:
        tick = tick_entry.get("tick")
        players_data = tick_entry.get("data", {}).get("players", {})

        # 1) 更新当前 tick 内所有玩家的位置信息、朝向、ping 等
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
                    last_known[player]["rot"] = (updated["yaw"], updated["pitch"])

                if event_type == "PlayerUpdatedPing":
                    last_known.setdefault(player, {})
                    last_known[player]["ping"] = updated.get("ping", "")

        # 2) 提取 player_ecid 的攻击事件
        if player_ecid in players_data:
            for event in players_data[player_ecid]:
                updated = event.get("updated", {})
                # 攻击事件
                if "attackTarget" in updated:
                    target_entity = updated["attackTarget"]
                    target_player = pair_dict.get(target_entity)  # entityID -> 玩家名

                    attacker_data = last_known.get(player_ecid, {})
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
                        "target_player": target_player,  # 目标玩家的“名字”
                        "target_yaw": target_rot[0] if target_rot else "",
                        "target_pitch": target_rot[1] if target_rot else "",
                        "target_ping": target_ping,
                        "distance": distance
                    }
                    results.append(record)
                    # 如果只想记录一次攻击，可以 break；如果要记录多个连续攻击，请注释掉 break
                    # break
    return results


def write_attack_events(records, csv_filepath):
    """
    将攻击事件写入 CSV。若没有攻击事件，则返回 False。
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
    """读取 .avro + .avsc 文件"""
    with open(schema_filepath, "r", encoding="utf-8") as schema_file:
        schema = json.load(schema_file)
    with open(avro_filepath, "rb") as f:
        avro_output = schemaless_reader(f, schema)
    return avro_output


def metadata_reader(metadata_filepath):
    """读取 .metadata.json 文件"""
    with open(metadata_filepath, "r", encoding="utf-8") as f:
        metadata = json.load(f)
    return metadata


def pair_entity_id(metadata_dict):
    """
    根据 metadata 中的玩家信息，生成 { entityID: playerName } 的映射。
    """
    pair_dict = {}
    for player in metadata_dict.get("players", []):
        pair_dict[player["entityID"]] = player["name"]
    return pair_dict


def process_replay_files_for_all_players(avro_dir, output_dir):
    """
    遍历 avro_dir 下所有 .avro 回放文件：
      1) 打开对应的 .avsc, .metadata.json
      2) 获取该回放包含的所有玩家
      3) 对每个玩家生成一个 CSV (输出到 output_dir)
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    lack_schema = 0
    success = 0
    no_attack_data = 0

    for filename in os.listdir(avro_dir):
        if filename.endswith(".avro"):
            # 回放号 (去掉 .avro 后缀)
            replay_id = filename[:-5]
            avro_filepath = os.path.join(avro_dir, filename)
            schema_filepath = os.path.join(avro_dir, replay_id + ".avsc")
            metadata_filepath = os.path.join(avro_dir, replay_id + ".metadata.json")

            if not (os.path.exists(schema_filepath) and os.path.exists(metadata_filepath)):
                print(f"Lack schema or metadata file for {replay_id}, skipping...")
                lack_schema += 1
                continue

            # 读取 avro, metadata
            avro_data = avro_reader(avro_filepath, schema_filepath)
            metadata = metadata_reader(metadata_filepath)
            pair_dict = pair_entity_id(metadata)

            # 遍历该回放中的所有玩家
            players_info = metadata.get("players", [])
            for p in players_info:
                player_ecid = p["entityID"]  # 攻击者的 entityID
                records = process_attack_events(avro_data, player_ecid, pair_dict)

                # 输出文件名：{回放号}_{玩家entityID}.csv
                csv_filename = f"{replay_id}_{player_ecid}.csv"
                csv_filepath = os.path.join(output_dir, csv_filename)

                if write_attack_events(records, csv_filepath):
                    success += 1
                else:
                    no_attack_data += 1

    print("Lack of schema or metadata files:", lack_schema)
    print("Successfully processed files:", success)
    print("Files (player-CSV) without attack data:", no_attack_data)


if __name__ == "__main__":
    # 示例：将输出放在 data/original_csv/test 下
    base_avro_dir = "data/avro_data/test"  # 这是playback-process的小工具导出的文件
    base_output_dir = "data/original_csv/test"
    print(f"Start processing: {base_avro_dir}")
    process_replay_files_for_all_players(base_avro_dir, base_output_dir)
