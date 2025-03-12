import csv
import json
import os
import shutil

from fastavro import schemaless_reader

# 默认参数（没有调整过的）
DEFAULT_HACK = {
    "kbH": 29,
    "kbV": 29,
    "hitbox": 100,
    "speed": 100
}


def is_default(hack):
    """
    检查 hack 参数是否为默认值
    :param hack: 玩家的 hack 参数
    :return: 是否为默认值
    """
    return all(hack.get(key, DEFAULT_HACK[key]) == DEFAULT_HACK[key] for key in DEFAULT_HACK)


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


def contains_attack_in_avro(avro_filepath, avsc_filepath):
    """
    检查 avro 文件中是否包含攻击事件
    :param avro_filepath: avro 文件路径
    :param avsc_filepath: schema 文件路径(avsc文件)
    :return:
    """
    try:
        avro_data = avro_reader(avro_filepath, avsc_filepath)
        ticks = avro_data.get("ticks", [])
        for tick_entry in ticks:
            players_data = tick_entry.get("data", {}).get("players", {})
            for player, events in players_data.items():
                for event in events:
                    updated = event.get("updated", {})
                    if "attackTarget" in updated:
                        return True
        return False
    except Exception as e:
        print(f"读取 Avro 文件失败: {e}")
        return False


def get_player_pair(metadata_dict):
    """
    从 metadata 中提取所有玩家的名字，排序后返回元组。
    :param metadata_dict: metadata
    :return: 两位玩家名的元组
    """
    players = metadata_dict.get("players", [])
    names = [p["name"] for p in players]
    if len(names) < 2:
        return None
    return tuple(sorted(names))


def get_modification_info(metadata_dict):
    """
    检查 metadata 中玩家 hack 参数的修改情况：
    遍历 players，找出 hack 参数不为默认的玩家。
    如果不是恰好一位玩家修改了参数，则返回 (False, None, None)。
    否则，判断其修改类型。

    :param metadata_dict: metadata
    :return: (是否只有一位玩家修改参数, 修改玩家名字, 修改参数的名字)
    """
    players = metadata_dict.get("players", [])
    modified = []
    for player in players:
        hack = player.get("hack", {})
        if not is_default(hack):
            mods = []
            if hack.get("hitbox", DEFAULT_HACK["hitbox"]) != DEFAULT_HACK["hitbox"]:
                mods.append("hitbox")
            if hack.get("speed", DEFAULT_HACK["speed"]) != DEFAULT_HACK["speed"]:
                mods.append("speed")
            modified.append((player["name"], mods))
    if len(modified) != 1:
        return False, None, None
    name, mods = modified[0]
    if "hitbox" in mods:
        hack_type = "reach"
    elif "speed" in mods:
        hack_type = "speed"
    else:
        hack_type = None
    return True, name, hack_type


def move_files(src_directory, files, dest_directory):
    """
    将指定文件从 src_directory 移动到 dest_directory，
    :param src_directory: 源路径
    :param files: 文件
    :param dest_directory: 目标路径
    :return:
    """
    if not os.path.exists(dest_directory):
        os.makedirs(dest_directory, exist_ok=True)
    for f in files:
        src_path = os.path.join(src_directory, f)
        if os.path.exists(src_path):
            try:
                shutil.move(src_path, dest_directory)
                print(f"移动文件: {f} 到 {dest_directory}")
            except Exception as e:
                print(f"移动文件 {f} 失败: {e}")


def process_replay_files(directory):
    """
    处理目录下所有回放，按以下逻辑：
      1) 读取每个回放的三件套文件（metadata, avro, avsc）。
      2) 检查 metadata 中自定义参数修改情况：必须恰好一位玩家修改了参数，
         否则将文件移动到 metadata_error 文件夹（子文件夹以回放编号命名）。
      3) 如果 metadata 检查通过，再检查 avro 文件是否存在且包含攻击事件，
         不符合则移动到 no_attack_data 文件夹（子文件夹以回放编号命名）。
      4) 如果以上均通过，则根据修改的 hack 参数确定：
         - hitbox 修改 → 移动到 reach/<修改玩家名>/ 目录下；
         - speed 修改 → 移动到 speed/<修改玩家名>/ 目录下。
    :param directory: 回放数据目录（包含 metadata, avro, avsc 三件套）
    :return: 处理统计结果的字典
    """
    aggregated = {}
    all_files = os.listdir(directory)

    # 按回放号分组
    base_groups = {}
    for f in all_files:
        base = f.split('.')[0]
        base_groups.setdefault(base, []).append(f)

    for base, files in base_groups.items():
        metadata_file = f"{base}.metadata.json"
        avro_file = f"{base}.avro"
        avsc_file = f"{base}.avsc"

        metadata_path = os.path.join(directory, metadata_file)
        if metadata_file not in files or not os.path.isfile(metadata_path):
            continue  # 缺少 metadata，不处理

        try:
            with open(metadata_path, "r", encoding="utf-8") as m:
                metadata_dict = json.load(m)
        except Exception as e:
            print(f"解析 metadata 失败: {metadata_file} 错误: {e}")
            continue

        # 获取玩家组合
        pair = get_player_pair(metadata_dict)
        if not pair:
            continue

        # 初始化统计信息
        if pair not in aggregated:
            aggregated[pair] = {
                "valid": 0,
                "metadata_issue": 0,
                "no_attack_issue": 0,
                "success_ids": [],
                "invalid_ids": []
            }

        # 检查自定义参数修改情况
        valid_meta, mod_player, hack_type = get_modification_info(metadata_dict)
        if not valid_meta or hack_type is None:
            aggregated[pair]["metadata_issue"] += 1
            aggregated[pair]["invalid_ids"].append(base)
            dest = os.path.join(directory, "metadata_error", base)
            move_files(directory, files, dest)
            continue

        # 检查 avro 与 avsc 文件是否存在
        avro_path = os.path.join(directory, avro_file)
        avsc_path = os.path.join(directory, avsc_file)
        if (avro_file not in files or avsc_file not in files or
                not os.path.isfile(avro_path) or not os.path.isfile(avsc_path)):
            aggregated[pair]["no_attack_issue"] += 1
            aggregated[pair]["invalid_ids"].append(base)
            dest = os.path.join(directory, "no_attack_data", base)
            move_files(directory, files, dest)
            continue

        # 检查 avro 内容是否包含攻击事件
        if not contains_attack_in_avro(avro_path, avsc_path):
            aggregated[pair]["no_attack_issue"] += 1
            aggregated[pair]["invalid_ids"].append(base)
            dest = os.path.join(directory, "no_attack_data", base)
            move_files(directory, files, dest)
            continue

        # 如果所有检查通过，则认为回放有效
        aggregated[pair]["valid"] += 1
        aggregated[pair]["success_ids"].append(base)
        # 根据修改类型将文件移动到对应目录下
        dest = os.path.join(directory, hack_type, mod_player)
        move_files(directory, files, dest)

    return aggregated


def export_csv(aggregated, output_path):
    """
    将统计结果导出为 CSV 文件，字段包括：
    - 玩家1
    - 玩家2
    - 成功数
    - 错误Meta数
    - 无攻击数
    - 成功回放号
    - 无效回放号
    :param aggregated: 统计结果字典
    :param output_path: csv文件输出路径
    :return: 不返回，直接写入一个储存处理结果的csv文件，包含以下字段：
        - 玩家1
        - 玩家2
        - 成功数
        - 错误Meta数
        - 无攻击数
        - 成功回放号
        - 无效回放号
    """
    with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
        fieldnames = ["玩家1", "玩家2", "成功数", "错误Meta数", "无攻击数", "成功回放号", "无效回放号"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for pair, stats in aggregated.items():
            writer.writerow({
                "玩家1": pair[0],
                "玩家2": pair[1],
                "成功数": stats["valid"],
                "错误Meta数": stats["metadata_issue"],
                "无攻击数": stats["no_attack_issue"],
                "成功回放号": ",".join(stats["success_ids"]),
                "无效回放号": ",".join(stats["invalid_ids"])
            })


def main(directory, output_path):
    aggregated = process_replay_files(directory)

    # 打印统计结果
    print("\n========== 玩家组合回放统计结果 ==========")
    for pair, stats in aggregated.items():
        print(f"玩家组合: {pair[0]} & {pair[1]}")
        print(f"  成功局数: {stats['valid']}")
        print(f"  错误Meta局数: {stats['metadata_issue']}")
        print(f"  无攻击局数: {stats['no_attack_issue']}")
        print(f"  成功回放号: {', '.join(stats['success_ids'])}")
        print(f"  无效回放号: {', '.join(stats['invalid_ids'])}")
        print()

    # 导出 CSV 文件
    export_csv(aggregated, output_path)
    print(f"统计结果已导出至: {output_path}")


if __name__ == "__main__":
    base_avro_dir = "./data/avro_data/test"  # 回放数据目录
    output_csv_path = "./result.csv"  # 统计结果csv输出路径
    main(base_avro_dir, output_csv_path)
