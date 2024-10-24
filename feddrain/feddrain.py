from drain.Drain import LogParser as DrainLogParser
from drain.Drain import Logcluster
from typing import List
from abc import abstractmethod

class LogParser:
    def __init__(self):
        pass

    @abstractmethod
    def get_templates(self) -> List[List[str]]:
        pass
    

class LogMerger:
    
    def __init__(self, log_format,
        indir="./",
        outdir="./result/",
        depth=4,
        st=0.4,
        maxChild=100,
        rex=[],
        keep_para=False, delis=[], filt=[], log_name="", delimiter=None):

        self.log_format = log_format
        self.indir = indir
        self.outdir = outdir
        self.depth = depth
        self.st = st
        self.maxChild = maxChild
        self.rex = rex
        self.keep_para = keep_para
        self.log_name = log_name
        self.delis = delis
        self.filt = filt
        self.delimiter = delimiter
    
    
    def merge(self, parsers: List[LogParser], mode=None) -> DrainLogParser:
            if len(parsers) == 0:
                raise ValueError("can not parse a empty list of parsers")

            mergedParser = DrainLogParser(log_format=self.log_format, 
                                            indir=self.indir,
                                            outdir=self.outdir,
                                            depth=self.depth,
                                            st=self.st,
                                            maxChild=self.maxChild,
                                            rex=self.rex,
                                            keep_para=self.keep_para, 
                                            log_name=self.log_name,
                                            afilt=self.filt,
                                            delis=self.delis,
                                            delimiter=self.delimiter)

            # 将每个parser的模板作为一组log提取出来
            all_templates = []
            template_set = set()
            for tparser in parsers:
                new_templates = tparser.get_templates()
                for temp in new_templates:
                    temp_str = ' '.join(temp)
                    if mode is not None:
                        temp_str = mergedParser.preprocess(temp_str)
                        temp = mergedParser.split_line(temp_str)
                        temp_str = ' '.join(temp)
                    if temp_str in template_set:
                        continue
                    all_templates.append(temp)
                    template_set.add(temp_str)
                    
            
            clusterL = []
            rootNode = mergedParser.root
            import tqdm
            for template in tqdm.tqdm(all_templates):
                matchCluster = mergedParser.treeSearch(rootNode, template)
                
                if matchCluster is None:
                    # 插入新的模板
                    newCluster = Logcluster(logTemplate=template, logIDL=[])
                    mergedParser.addSeqToPrefixTree(rootNode, newCluster)
                    clusterL.append(newCluster)
                else:                   
                    newTemplate = mergedParser.getTemplate(template, matchCluster.logTemplate)
                    if " ".join(newTemplate) != " ".join(matchCluster.logTemplate):
                        matchCluster.logTemplate = newTemplate
            return mergedParser