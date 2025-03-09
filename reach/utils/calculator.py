import math


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
