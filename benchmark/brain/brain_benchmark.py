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
from brain.brain import LogParser
from utils import evaluator
import pandas as pd
import os

input_dir = "../../../dataset/"  # The input directory of log file
output_dir = "Brain_result_big/"  # The output directory of parsing results

benchmark_settings = {
    "HDFS": {
        "log_file": "HDFS/HDFS.log",
        "log_format": "<Date> <Time> <Pid> <Level> <Component>: <Content>",
        "regex": [r"blk_-?\d+", r"(\d+\.){3}\d+(:\d+)?"],
        "delimiter": [""],
        "theshold": 2,
    },
    "BGL": {
        "log_file": "BGL/BGL.log",
        "log_format": "<Label> <Timestamp> <Date> <Node> <Time> <NodeRepeat> <Type> <Component> <Level> <Content>",
        "regex": [r"core\.\d+"],
        "delimiter": [],
        "theshold": 6,
    }
}

benchmark_result = []

for dataset, setting in benchmark_settings.items():
    print("\n=== Evaluation on %s ===" % dataset)
    indir = os.path.join(input_dir, os.path.dirname(setting["log_file"]))
    log_file = os.path.basename(setting["log_file"])
    parser = LogParser(
        log_format=setting["log_format"],
        indir=indir,
        outdir=output_dir,
        rex=setting["regex"],
        delimeter=setting["delimiter"],
        threshold=setting["theshold"],
        logname=dataset,
    )
    parser.parse(log_file)

    F1_measure, accuracy = evaluator.evaluate(
        groundtruth=os.path.join(indir, log_file + "_structured.csv"),
        parsedresult=os.path.join(output_dir, log_file + "_structured.csv"),
    )
    benchmark_result.append([dataset, F1_measure, accuracy])

print("\n=== Overall evaluation results ===")
df_result = pd.DataFrame(
    benchmark_result, columns=["Dataset", "F1_measure", "Accuracy"]
)
df_result.set_index("Dataset", inplace=True)
print(df_result)
df_result.to_csv("Brain_bechmark_result.csv", float_format="%.6f")
