import sys
sys.path.append("../../")
from drain.Drain import LogParser
from utils import evaluator
from utils.dataset_splitter import split_chunk
import os
import pandas as pd


input_dir = "../../data/splitted/"  # The input directory of log file
input_dir_test = "../../../dataset/"
output_dir = "Drain_result/"  # The output directory of parsing results
chunk_num = 5

split_chunk(input_dir_test, input_dir, chunk_num, True)
benchmark_settings = {
    "HDFS": {
        "log_file": "HDFS/HDFS.log",
        "log_format": "<Date> <Time> <Pid> <Level> <Component>: <Content>",
        "regex": [r"blk_-?\d+", r"(\d+\.){3}\d+(:\d+)?"],
        "st": 0.5,
        "depth": 4,
    },
    "BGL": {
        "log_file": "BGL/BGL.log",
        "log_format": "<Label> <Timestamp> <Date> <Node> <Time> <NodeRepeat> <Type> <Component> <Level> <Content>",
        "regex": [r"core\.\d+"],
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
    import gc

    parsers = [
        LogParser(
            log_format=setting["log_format"],
            indir=indir,
            outdir=output_dir,
            rex=setting["regex"],
            depth=setting["depth"],
            st=setting["st"],
        )
        for i in range(chunk_num)
    ]
    print(f'gc collected {gc.collect()}')
    for tparser, file in zip(parsers, train_files):
        tparser.parse(file, output=False)
        print(f'gc collected {gc.collect()}')
    
    from feddrain.merger2 import LogMerger
    in_dir_test = os.path.join(input_dir_test, os.path.dirname(setting["log_file"]))
    mergedParser = LogMerger().merge(parsers, in_dir_test, output_dir)
    mergedParser.parse(log_file, parse_only=True)
    # parser.parse(log_file)

    print(f'gc collected {gc.collect()}')
    F1_measure, accuracy = evaluator.evaluate(
        groundtruth=os.path.join(in_dir_test, log_file + "_structured.csv"),
        parsedresult=os.path.join(output_dir, log_file + "_structured.csv"),
    )
    bechmark_result.append([dataset, F1_measure, accuracy])


print("\n=== Overall evaluation results ===")
df_result = pd.DataFrame(bechmark_result, columns=["Dataset", "F1_measure", "Accuracy"])
df_result.set_index("Dataset", inplace=True)
print(df_result)
df_result.to_csv("Drain_bechmark_result.csv", float_format="%.6f")