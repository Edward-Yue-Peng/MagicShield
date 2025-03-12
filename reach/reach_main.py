from prediction.reach_predictor import *
from training.preprocess_reach_csv import *
from training.train_reach_model import *
from utils.convert_csv import *


def convert_csv_for_training():
    base_avro_dir = "./data/avro_data"  # 这是 playback-process 小工具导出的文件
    base_output_dir = "./data/original_csv"  # 输出的原始 csv

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


def convert_csv_for_test(ecid):
    base_avro_dir = "./data/avro_data/test"  # 这是playback-process的小工具导出的文件
    base_output_dir = "./data/original_csv/test"  # 输出的原始csv
    process_replay_files(base_avro_dir, base_output_dir, ecid)


# 测试：将avro数据转换为原始csv，筛选特定ecid
# convert_csv_for_test("demo")

# 第一步：将 avro 数据转换为原始 csv
print("======Converting avro data to csv...======")
convert_csv_for_training()

# 第二步：将原始 csv 切分为段
print("======Preprocessing csv files...======")
preprocess_reach_csv(min_ticks_per_segment=8)

# 第三步：训练模型
print("======Training model...======")
train_reach(threshold=0.7)

# 第四步：验证模型
predict_reach(0.7, 8)
