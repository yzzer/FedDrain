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
import os
import pandas as pd


input_dir = "../../../dataset/"  # The input directory of log file
output_dir = "Spell_result/"  # The output directory of parsing results

benchmark_settings = {
    "HDFS": {
        "log_file": "HDFS/HDFS.log",
        "log_format": "<Date> <Time> <Pid> <Level> <Component>: <Content>",
        "regex": [r"\/(?:[a-zA-Z0-9_.-]+\/)+[a-zA-Z0-9_.-]+", r"blk_-?\d+", r"/(\d+\.){3}\d+(:\d+)?", r"(\d+\.){3}\d+(:\d+)?"],
        "tau": 0.7,
    },
    "BGL": {
        "log_file": "BGL/BGL.log",
        "log_format": "<Label> <Timestamp> <Date> <Node> <Time> <NodeRepeat> <Type> <Component> <Level> <Content>",
        "regex": [r"\/(?:[a-zA-Z0-9_.-]+\/)+[a-zA-Z0-9_.-]+", r"core\.\d+", ],
        "tau": 0.75,
    }
}

bechmark_result = []
for dataset, setting in benchmark_settings.items():
    print("\n=== Evaluation on %s ===" % dataset)
    indir = os.path.join(input_dir, os.path.dirname(setting["log_file"]))
    log_file = os.path.basename(setting["log_file"])

    parser = LogParser(
        log_format=setting["log_format"],
        indir=indir,
        outdir=output_dir,
        rex=setting["regex"],
        tau=setting["tau"],
    )
    parser.parse(log_file)
    import gc
    print(f'gc collected {gc.collect()}')

    F1_measure, accuracy = evaluator.evaluate(
        groundtruth=os.path.join(indir, log_file + "_structured.csv"),
        parsedresult=os.path.join(output_dir, log_file + "_structured.csv"),
    )
    bechmark_result.append([dataset, F1_measure, accuracy])
    print(f'gc collected {gc.collect()}')

print("\n=== Overall evaluation results ===")
df_result = pd.DataFrame(bechmark_result, columns=["Dataset", "F1_measure", "Accuracy"])
df_result.set_index("Dataset", inplace=True)
print(df_result)
df_result.to_csv("Spell_bechmark_result.csv", float_format="%.6f")
