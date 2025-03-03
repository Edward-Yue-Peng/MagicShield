import csv
import json
import math
import os


def calculate_distance(pos1, pos2):
    return round(math.sqrt(
        (pos1["x"] - pos2["x"]) ** 2
        + (pos1["y"] - pos2["y"]) ** 2
        + (pos1["z"] - pos2["z"]) ** 2
    ), 3)


def find_nearest_value(playback, player, tick, key, search_range=20, direction=1):
    for offset in range(search_range + 1):
        check_tick = str(int(tick) + offset * direction)
        if check_tick in playback.get(player, {}) and key in playback[player][check_tick]:
            return playback[player][check_tick][key]
    return None


def process_data(data, output_file):
    target = data["training"]["target"]
    session_info = data.get("sessionInfo", {})
    players = session_info.get("players", [])

    # 确定对手的 ID
    opponent = None
    for player in players:
        if player["name"] != target:
            opponent = player["name"]
            break

    playback = data["playback"]
    if target not in playback or not opponent:
        return

    results = []
    attack_streak = []

    for tick, events in playback[target].items():
        if "PlayerUpdatedAttack" in events:
            attack_target_id = events["PlayerUpdatedAttack"].get("attackTarget")
            target_position = events.get("PlayerUpdatedPositionXYZ")
            target_ping = find_nearest_value(playback, target, tick, "PlayerUpdatedPing")
            target_rotation = find_nearest_value(playback, target, tick, "PlayerUpdatedRotation")

            opponent_position = None
            if tick in playback.get(opponent, {}):
                opponent_events = playback[opponent][tick]
                if "PlayerUpdatedPositionXYZ" in opponent_events:
                    opponent_position = opponent_events["PlayerUpdatedPositionXYZ"]

            if target_position and opponent_position:
                distance = calculate_distance(target_position, opponent_position)
                if distance > 3:
                    attack_streak.append((tick, distance, target_ping, target_rotation))
                else:
                    if len(attack_streak) >= 5:
                        results.extend(attack_streak)
                    attack_streak = []

    if len(attack_streak) >= 5:
        results.extend(attack_streak)

    with open(output_file, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Tick", "Distance", "Ping", "Rotation"])
        writer.writerows(results)


def process_all_json_files(input_folder, output_folder):
    processed_folders = set()
    total_files = sum([len(files) for _, _, files in os.walk(input_folder) if files])
    processed_files = 0

    for root, _, files in os.walk(input_folder):
        if files:
            print(f"Processing folder: {root}")
            processed_folders.add(root)

        for file in files:
            if file.endswith(".json"):
                json_path = os.path.join(root, file)
                try:
                    with open(json_path, "r", encoding="utf-8") as json_file:
                        data = json.load(json_file)
                        training_type = data["training"].get("type", "normal")
                        output_subfolder = os.path.join(output_folder, training_type)
                        os.makedirs(output_subfolder, exist_ok=True)
                        csv_filename = os.path.join(output_subfolder, file.replace(".json", ".csv"))
                        process_data(data, csv_filename)
                        processed_files += 1
                        print(f"Progress: {processed_files}/{total_files} files processed")
                except (FileNotFoundError, json.JSONDecodeError) as e:
                    print(f"Error processing {json_path}: {e}")

    for folder in processed_folders:
        print(f"Processed folder: {folder}")


input_directory = "data/replayJson"
output_directory = "data/processCSV"
process_all_json_files(input_directory, output_directory)
