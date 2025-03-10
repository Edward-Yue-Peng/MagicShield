import json
import os

DEFAULT_HACK = {
    "kbH": 29,
    "kbV": 29,
    "hitbox": 100,
    "speed": 100
}


def is_default(hack):
    """检查 hack 参数是否为默认值"""
    return all(hack.get(key, default) == default for key, default in DEFAULT_HACK.items())


def process_json_files(directory):
    valid_files = {}  # 存放有效文件信息，按玩家分组
    invalid_counts = {}  # 存放无效文件数量，按玩家计数

    json_files = [f for f in os.listdir(directory) if
                  f.endswith(".json") and os.path.isfile(os.path.join(directory, f))]

    base_groups = {}
    for filename in json_files:
        base = filename.split('.')[0]
        base_groups.setdefault(base, []).append(filename)

    # 针对每个组进行处理
    for base, files in base_groups.items():
        group_is_valid = True
        group_valid_details = []  # 保存该组中每个有效 JSON 文件的详情
        group_invalid_details = []  # 保存该组中无效文件的玩家信息（如果能获取到）

        # 检查该组内的每个 JSON 文件
        for filename in files:
            filepath = os.path.join(directory, filename)
            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception as e:
                # JSON 解析失败，视为无效
                group_is_valid = False
                # 无法获取玩家信息，直接跳过计数
                continue

            # 检查 game 字段
            if data.get("game", "") != "buhc_diy":
                group_is_valid = False
                players = data.get("players", [])
                # 如果有玩家调整，则记录无效信息
                adjusted = [p for p in players if not is_default(p.get("hack", {}))]
                for p in adjusted:
                    player = p.get("name", "未知")
                    group_invalid_details.append((player, filename))
                continue

            players = data.get("players", [])
            # 筛选出 hack 参数不为默认的玩家
            adjusted = [p for p in players if not is_default(p.get("hack", {}))]
            if len(adjusted) == 1:
                # 该 JSON 文件符合要求，记录详细信息
                p = adjusted[0]
                player = p.get("name", "未知")
                hack = p.get("hack", {})
                group_valid_details.append({
                    "player": player,
                    "filename": filename,
                    "hitbox": hack.get("hitbox"),
                    "speed": hack.get("speed")
                })
            else:
                # 调整玩家数不等于 1，视为无效
                group_is_valid = False
                if adjusted:
                    for p in adjusted:
                        player = p.get("name", "未知")
                        group_invalid_details.append((player, filename))
                # 如果 adjusted 为空，则无法获取玩家信息

        if group_is_valid and group_valid_details:
            # 整个组均有效，则将该组内的文件记录到 valid_files 中
            for detail in group_valid_details:
                player = detail["player"]
                valid_files.setdefault(player, []).append(detail)
        else:
            # 组内存在无效文件，则删除整个组对应的所有文件（匹配文件名前缀）
            for f in os.listdir(directory):
                if f.startswith(base):
                    try:
                        os.remove(os.path.join(directory, f))
                        print(f"删除文件: {f}")
                    except Exception as e:
                        print(f"删除文件 {f} 失败: {e}")
            # 累计无效文件计数（仅对能提取到玩家信息的文件计数）
            for player, filename in group_invalid_details:
                invalid_counts[player] = invalid_counts.get(player, 0) + 1

    return valid_files, invalid_counts


def main_workflow(directory):
    valid_files, invalid_counts = process_json_files(directory)

    # 合并所有出现过的玩家
    all_players = set(valid_files.keys()) | set(invalid_counts.keys())
    print("\n玩家 hack 分析结果:")
    for player in sorted(all_players):
        valid_count = len(valid_files.get(player, []))
        invalid_count = invalid_counts.get(player, 0)
        print(f"玩家: {player}，{valid_count}份有效文件，{invalid_count}份无效文件")
        for item in valid_files.get(player, []):
            print(f"文件: {item['filename']}，Hitbox: {item['hitbox']}, Speed: {item['speed']}")
        print()


if __name__ == "__main__":
    target_dir = "/Users/pengyue/Documents/GitHub/MagicShield/reach/data/avro_data/test"
    if not os.path.exists(target_dir):
        print(f"目录不存在: {target_dir}")
        exit()
    if not os.path.isdir(target_dir):
        print(f"路径不是目录: {target_dir}")
        exit()
    main_workflow(target_dir)
