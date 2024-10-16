#!/usr/bin/env python

import sys
sys.path.append('.')
from drain.Drain import LogParser

input_dir  = './data/splitted/HDFS/' # The input directory of log file
output_dir = 'demo_result/'  # The output directory of parsing results
input_dir_test = './data/loghub_2k/HDFS/'
log_file   = 'HDFS_2k.log'  # The input log file name
chunk_num = 5
log_format = '<Date> <Time> <Pid> <Level> <Component>: <Content>'  # HDFS log format
# Regular expression list for optional preprocessing (default: [])
regex      = [
    r'blk_(|-)[0-9]+' , # block id
    r'(/|)([0-9]+\.){3}[0-9]+(:[0-9]+|)(:|)', # IP
    r'(?<=[^A-Za-z0-9])(\-?\+?\d+)(?=[^A-Za-z0-9])|[0-9]+$', # Numbers
]
st         = 0.5  # Similarity threshold
depth      = 4  # Depth of all leaf nodes

file_name, file_type = log_file.split(".")
train_files = [
    f"{file_name}-chunk-{i}.{file_type}" for i in range(chunk_num)
]
# 单独训练不同的parser
parsers = [
    LogParser(log_format, indir=input_dir, outdir=output_dir,  depth=depth, st=st, rex=regex)
    for i in range(chunk_num)
]
for parser, file in zip(parsers, train_files):
    parser.parse(file, output=False)
    
from feddrain.merger import LogMerger
mergedParser = LogMerger().merge(parsers, input_dir_test, output_dir)
mergedParser.parse(log_file, parse_only=True)