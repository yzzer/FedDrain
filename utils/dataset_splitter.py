import os
import gc
import shutil
from pathlib import Path


benchmark_settings = {
        "HDFS": {
            "log_file": "HDFS/HDFS_2k.log",
            "log_format": "<Date> <Time> <Pid> <Level> <Component>: <Content>",
            "regex": [r"blk_-?\d+", r"(\d+\.){3}\d+(:\d+)?"],
            "st": 0.5,
            "depth": 4,
        },
        "Hadoop": {
            "log_file": "Hadoop/Hadoop_2k.log",
            "log_format": "<Date> <Time> <Level> \[<Process>\] <Component>: <Content>",
            "regex": [r"(\d+\.){3}\d+"],
            "st": 0.5,
            "depth": 4,
        },
        "Spark": {
            "log_file": "Spark/Spark_2k.log",
            "log_format": "<Date> <Time> <Level> <Component>: <Content>",
            "regex": [r"(\d+\.){3}\d+", r"\b[KGTM]?B\b", r"([\w-]+\.){2,}[\w-]+"],
            "st": 0.5,
            "depth": 4,
        },
        "Zookeeper": {
            "log_file": "Zookeeper/Zookeeper_2k.log",
            "log_format": "<Date> <Time> - <Level>  \[<Node>:<Component>@<Id>\] - <Content>",
            "regex": [r"(/|)(\d+\.){3}\d+(:\d+)?"],
            "st": 0.5,
            "depth": 4,
        },
        "BGL": {
            "log_file": "BGL/BGL_2k.log",
            "log_format": "<Label> <Timestamp> <Date> <Node> <Time> <NodeRepeat> <Type> <Component> <Level> <Content>",
            "regex": [r"core\.\d+"],
            "st": 0.5,
            "depth": 4,
        },
        "HPC": {
            "log_file": "HPC/HPC_2k.log",
            "log_format": "<LogId> <Node> <Component> <State> <Time> <Flag> <Content>",
            "regex": [r"=\d+"],
            "st": 0.5,
            "depth": 4,
        },
        "Thunderbird": {
            "log_file": "Thunderbird/Thunderbird_2k.log",
            "log_format": "<Label> <Timestamp> <Date> <User> <Month> <Day> <Time> <Location> <Component>(\[<PID>\])?: <Content>",
            "regex": [r"(\d+\.){3}\d+"],
            "st": 0.5,
            "depth": 4,
        },
        "Windows": {
            "log_file": "Windows/Windows_2k.log",
            "log_format": "<Date> <Time>, <Level>                  <Component>    <Content>",
            "regex": [r"0x.*?\s"],
            "st": 0.7,
            "depth": 5,
        },
        "Linux": {
            "log_file": "Linux/Linux_2k.log",
            "log_format": "<Month> <Date> <Time> <Level> <Component>(\[<PID>\])?: <Content>",
            "regex": [r"(\d+\.){3}\d+", r"\d{2}:\d{2}:\d{2}"],
            "st": 0.39,
            "depth": 6,
        },
        "Android": {
            "log_file": "Android/Android_2k.log",
            "log_format": "<Date> <Time>  <Pid>  <Tid> <Level> <Component>: <Content>",
            "regex": [
                r"(/[\w-]+)+",
                r"([\w-]+\.){2,}[\w-]+",
                r"\b(\-?\+?\d+)\b|\b0[Xx][a-fA-F\d]+\b|\b[a-fA-F\d]{4,}\b",
            ],
            "st": 0.2,
            "depth": 6,
        },
        "HealthApp": {
            "log_file": "HealthApp/HealthApp_2k.log",
            "log_format": "<Time>\|<Component>\|<Pid>\|<Content>",
            "regex": [],
            "st": 0.2,
            "depth": 4,
        },
        "Apache": {
            "log_file": "Apache/Apache_2k.log",
            "log_format": "\[<Time>\] \[<Level>\] <Content>",
            "regex": [r"(\d+\.){3}\d+"],
            "st": 0.5,
            "depth": 4,
        },
        "Proxifier": {
            "log_file": "Proxifier/Proxifier_2k.log",
            "log_format": "\[<Time>\] <Program> - <Content>",
            "regex": [
                r"<\d+\ssec",
                r"([\w-]+\.)+[\w-]+(:\d+)?",
                r"\d{2}:\d{2}(:\d{2})*",
                r"[KGTM]B",
            ],
            "st": 0.6,
            "depth": 3,
        },
        "OpenSSH": {
            "log_file": "OpenSSH/OpenSSH_2k.log",
            "log_format": "<Date> <Day> <Time> <Component> sshd\[<Pid>\]: <Content>",
            "regex": [r"(\d+\.){3}\d+", r"([\w-]+\.){2,}[\w-]+"],
            "st": 0.6,
            "depth": 5,
        },
        "OpenStack": {
            "log_file": "OpenStack/OpenStack_2k.log",
            "log_format": "<Logrecord> <Date> <Time> <Pid> <Level> <Component> \[<ADDR>\] <Content>",
            "regex": [r"((\d+\.){3}\d+,?)+", r"/.+?\s", r"\d+"],
            "st": 0.5,
            "depth": 5,
        },
        "Mac": {
            "log_file": "Mac/Mac_2k.log",
            "log_format": "<Month>  <Date> <Time> <User> <Component>\[<PID>\]( \(<Address>\))?: <Content>",
            "regex": [r"([\w-]+\.){2,}[\w-]+"],
            "st": 0.7,
            "depth": 6,
        },
    }

def split_dataset(path, target_path, shard_num: int = 3, big_file: bool = False):
    if big_file:
        path = path.replace("_2k", "")
    if not os.path.exists(path):
        return
    if not os.path.exists(target_path):
        os.makedirs(target_path)
    datasets = [[] for i in range(shard_num)]
    with open(path, "r") as source:
        idx = 0
        for line in source.readlines():
            datasets[idx % shard_num].append(line)
            idx += 1
        print(f"read {idx} lines from {path}")
    
    file_name, file_type = Path(path).name.split(".")
    for idx, dataset in enumerate(datasets):
        with open(os.path.join(target_path, f'{file_name}-chunk-{idx}.{file_type}'), "w+") as target:
            target.writelines(dataset)
    del datasets
    gc.collect()
    

def split_chunk(input_dir='../data/loghub_2k/', output_dir='../data/splitted/', chunk_num=3, big=False):
    
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    for dataset_name, dataset_info in benchmark_settings.items():
        print(f"processing {dataset_name} to {chunk_num} chunks")
        target_path = os.path.join(output_dir, Path(dataset_info["log_file"]).parent)
        split_dataset(os.path.join(input_dir, dataset_info["log_file"]), target_path, shard_num=chunk_num, big_file=big)