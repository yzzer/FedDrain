# =========================================================================
# Copyright (C) 2016-2023 LOGPAI (https://github.com/logpai).
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# =========================================================================

import sys

sys.path.append("../../")
from spell.spell import LogParser
from utils import evaluator
from utils.dataset_splitter import split_chunk
import os

import pandas as pd

input_dir = "../../data/splitted—spell_10/"  # The input directory of log file
input_dir_test = "../../../dataset/"
output_dir = "Spell_result_fed_10/"  # The output directory of parsing results
chunk_num = 10

split_chunk(input_dir_test, input_dir, chunk_num, True)

benchmark_settings = {
    "HDFS": {
        "log_file": "HDFS/HDFS.log",
        "log_format": "<Date> <Time> <Pid> <Level> <Component>: <Content>",
        "regex": [r"\/(?:[a-zA-Z0-9_.-]+\/)+[a-zA-Z0-9_.-]+", r"blk_-?\d+", r"/(\d+\.){3}\d+(:\d+)?", r"(\d+\.){3}\d+(:\d+)?"],
        "tau": 0.8,
        "st": 0.5,
        "depth": 4,
    },
    "BGL": {
        "log_file": "BGL/BGL.log",
        "log_format": "<Label> <Timestamp> <Date> <Node> <Time> <NodeRepeat> <Type> <Component> <Level> <Content>",
        "regex": [r"\/(?:[a-zA-Z0-9_.-]+\/)+[a-zA-Z0-9_.-]+", r"core\.\d+"],
        "tau": 0.75,
         "st": 0.5,
        "depth": 4,
    },
}

bechmark_result = []
for dataset, setting in benchmark_settings.items():
    print("\n=== Evaluation on %s ===" % dataset)
    indir = os.path.join(input_dir, os.path.dirname(setting["log_file"]))
    log_file = os.path.basename(setting["log_file"])
    file_name, file_type = log_file.split(".")
    train_files = [
        f"{file_name}-chunk-{i}.{file_type}" for i in range(chunk_num)
    ]

    parsers = [
        LogParser(
            log_format=setting["log_format"],
            indir=indir,
            outdir=output_dir,
            rex=setting["regex"],
            tau=setting["tau"],
        )
        for i in range(chunk_num)
    ]
    import multiprocessing as mp

    def parse_file(tparser, file):
        tparser.parse(file, output=False)
        return tparser

    # 使用多进程来处理解析任务
    with mp.Pool(processes=min(mp.cpu_count() + 1, chunk_num)) as pool:  # 创建与 CPU 核数相同数量的进程
        parsers = pool.starmap(parse_file, zip(parsers, train_files))  # 并行处理任务
            
    
    from feddrain.feddrain import LogMerger
    in_dir_test = os.path.join(input_dir_test, os.path.dirname(setting["log_file"]))
    mergedParser = LogMerger(log_format=setting["log_format"],
            indir=in_dir_test,
            outdir=output_dir,
            rex=setting["regex"],
            depth=setting["depth"],
            st=setting["st"],
            log_name=dataset,).merge(parsers, mode=None)
    mergedParser.parse(log_file, parse_only=True)
    print("star eval...")
    F1_measure, accuracy = evaluator.evaluate(
        groundtruth=os.path.join(in_dir_test, log_file + "_structured.csv"),
        parsedresult=os.path.join(output_dir, log_file + "_structured.csv"),)
    bechmark_result.append([dataset, F1_measure, accuracy])

print("\n=== Overall evaluation results ===")
df_result = pd.DataFrame(bechmark_result, columns=["Dataset", "F1_measure", "Accuracy"])
df_result.set_index("Dataset", inplace=True)
print(df_result)
df_result.to_csv("Spell_bechmark_result_10.csv", float_format="%.6f")
