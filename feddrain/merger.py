from drain.Drain import LogParser, Logcluster, Node

from typing import List
import copy

class LogParserEncoder:
    def __init__(self):
        self.results = []
        
    def encrypt_parser(self, parser: LogParser) -> LogParser:
        pass
    
    def visit(self, node: Node):
        if node is None:
            return
        
        if not isinstance(node.childD, dict):
            for cluster in node.childD:
                template = copy.deepcopy(cluster.logTemplate)
                self.results.append(template)
        else:

            for key, child in node.childD.items():                 
                self.visit(child)

    
    def extract_log_messages_from_parser(self, parser: LogParser) -> List[List[str]]:
        self.results.clear()
        for _, node in parser.root.childD.items():
            self.visit(node)
        return self.results 


class LogMerger:
    
    def __init__(self):
        pass
    
    
    def merge(self, parsers: List[LogParser], indir, outdir) -> LogParser:
            if len(parsers) == 0:
                raise ValueError("can not parse a empty list of parsers")

            mergedParser = LogParser(parsers[0].log_format, indir, outdir, parsers[0].depth + 2, parsers[0].st, rex=parsers[0].rex)

            # 将每个parser的模板作为一组log提取出来
            all_templates = []
            template_set = set()
            encoder = LogParserEncoder()
            for tparser in parsers:
                new_templates = encoder.extract_log_messages_from_parser(tparser)
                for temp in new_templates:
                    temp_str = ' '.join(temp)
                    if temp_str in template_set:
                        continue
                    all_templates.append(temp)
                    template_set.add(temp_str)

            rootNode = mergedParser.root
            for template in all_templates:
                matchCluster = mergedParser.treeSearch(rootNode, template)
                if matchCluster is None:
                    # 插入新的模板
                    newCluster = Logcluster(logTemplate=template, logIDL=[])
                    mergedParser.addSeqToPrefixTree(rootNode, newCluster)
                else:                   
                    newTemplate = mergedParser.getTemplate(template, matchCluster.logTemplate)
                    if " ".join(newTemplate) != " ".join(matchCluster.logTemplate):
                        matchCluster.logTemplate = newTemplate
                        
            return mergedParser
    
