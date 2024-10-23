from spell.spell import LogParser, LCSObject, Node

from typing import List
import copy

class LogParserEncoder:
    def __init__(self):
        self.results = []
        
    def encrypt_parser(self, parser: LogParser) -> LogParser:
        pass

    
    def extract_log_messages_from_parser(self, parser: LogParser) -> List[List[str]]:
        self.results = [cluster.logTemplate for cluster in  parser.get_log_clu_list()]
        return self.results 


class LogMerger:
    
    def __init__(self):
        pass
    
    
    def merge(self, parsers: List[LogParser], indir, outdir) -> LogParser:
            if len(parsers) == 0:
                raise ValueError("can not parse a empty list of parsers")

            mergedParser = parsers[0]
            mergedParser.savePath = outdir
            mergedParser.path = indir
            # 将每个parser的模板作为一组log提取出来
            all_templates = []
            encoder = LogParserEncoder()
            
            for tparser in parsers[1:]:
                new_templates = encoder.extract_log_messages_from_parser(tparser)
                all_templates.extend(new_templates)
            
            rootNode = mergedParser.root
            logCluL = mergedParser.clustL
            for cluster in logCluL:
                cluster.logIDL.clear()

            import tqdm
            for template in tqdm.tqdm(all_templates):
                constLogMessL = [w for w in template if w != "<*>"]
                matchCluster = mergedParser.PrefixTreeMatch(rootNode, constLogMessL, 0)
                
                if matchCluster is None:
                    matchCluster = mergedParser.SimpleLoopMatch(logCluL, constLogMessL)

                    if matchCluster is None:
                        matchCluster = mergedParser.LCSMatch(logCluL, template)

                        if matchCluster is None:
                            newCluster = LCSObject(logTemplate=template, logIDL=[])
                            logCluL.append(newCluster)
                            mergedParser.addSeqToPrefixTree(rootNode, newCluster)
                        else:
                            newTemplate = mergedParser.getTemplate(
                                mergedParser.LCS(template, matchCluster.logTemplate),
                                matchCluster.logTemplate,
                            )
                            if " ".join(newTemplate) != " ".join(matchCluster.logTemplate):
                                mergedParser.removeSeqFromPrefixTree(rootNode, matchCluster)
                                matchCluster.logTemplate = newTemplate
                                mergedParser.addSeqToPrefixTree(rootNode, matchCluster)
            return mergedParser
    
