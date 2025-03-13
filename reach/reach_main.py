from prediction.reach_predictor import *
from reach.training.preprocess_reach_csv import preprocess_reach_csv
from reach.training.train_reach_model import train_reach
from reach.utils.convert_csv import process_replay_files, convert_csv_for_training

# 测试路径（对单个玩家判断）
test_avro_dir = "./data/avro_data/test"
test_csv_dir = "./data/original_csv/test"

# 预测路径（对大量玩家判断）
predict_avro_dir = "./data/avro_data/predict"
predict_csv_dir = "./data/original_csv/predict"

# 训练路径
avro_dir = "./data/avro_data"
output_dir = "./data/original_csv"

# 模型路径
model = "./model/reach_detect_1494_model.joblib"

# 预测报告输出路径
predict_report_dir = "./data/predict_report.csv"

# 错误分类输出路径
misclassified_dir = "./data/misclassified_data.csv"


def train():
    # 第一步：将 avro 数据转换为原始 csv
    print("======Converting avro data to csv...======")
    convert_csv_for_training(avro_dir, output_dir)
    # 第二步：将原始 csv 切分为段
    print("======Preprocessing csv files...======")
    preprocess_reach_csv(min_ticks_per_segment=8)
    # 第三步：训练模型
    print("======Training model...======")
    train_reach(0.7, misclassified_dir)


def test():
    # 测试：将avro数据转换为原始csv，筛选特定ecid
    process_replay_files(test_avro_dir, test_csv_dir, "ecid")
    # 验证模型
    predict_reach(test_csv_dir, model, 0.7, 8)


def predict():
    # 大数据量预测
    predict_reach_large_scale(predict_avro_dir, predict_csv_dir, predict_report_dir, model, 0.7, 8)


predict()
