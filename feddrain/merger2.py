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
        self.main_parser = None

    
    def deep_copy_node(self, node: Node):
        if node is None:
            return None

        new_node = Node(depth=node.depth, digitOrtoken=node.digitOrtoken)
        if not isinstance(node.childD, dict):
            new_node.childD = [
                Logcluster(logTemplate=clust.logTemplate, logIDL=clust.logIDL)
                for clust in node.childD
            ]
        else:
            new_node.childD = dict()
            for key, child in node.childD.items():
                new_node.childD[key] = self.deep_copy_node(child)
        return new_node
    

    def get_similarity(self, main_cluster: Logcluster, side_cluster: Logcluster):
        # if main_cluster.merge_time > 1:
        #     return 0.
        if len(main_cluster.logTemplate) != len(side_cluster.logTemplate):
            return 0.
        main_cluster.logIDL.clear()
        sim_tokens = 0
        for token1, token2 in zip(main_cluster.logTemplate, side_cluster.logTemplate):
            if token1 == token2 and token1 != "<*>":
                sim_tokens += 1
        return sim_tokens / (len(side_cluster.logTemplate) * 1.)

    
    def merge_cluster(self, main_clusters: List[Logcluster], side_clusters: List[Logcluster]):
        for side_cluster in side_clusters:
            has_similarity = False
            for main_cluster in main_clusters:
                if self.get_similarity(main_cluster, side_cluster) > self.main_parser.st:
                    main_cluster.logTemplate = self.main_parser.getTemplate(
                        main_cluster.logTemplate, side_cluster.logTemplate
                    )
                    main_cluster.merge_time += 1
                    has_similarity = True
            if not has_similarity:
                side_cluster.logIDL.clear()
                main_clusters.append(side_cluster)
            
    
    def merge_node(self, main_node: Node, side_node: Node):
        # 合并 log cluster
        if not isinstance(main_node.childD, dict):
            self.merge_cluster(main_node.childD, side_node.childD)
            return
        
        # 添加 main node 没有的 token
        exclude_tokens = set()
        for token, node in side_node.childD.items():
            
            if token not in main_node.childD:
                if  "<*>" in main_node.childD:
                    if len(main_node.childD) < self.main_parser.maxChild:
                        new_node = self.deep_copy_node(node)
                        main_node.childD[token] = new_node
                        exclude_tokens.add(token)
                    else:
                        self.merge_node(main_node.childD["<*>"], node)
                else:
                    if token == "<*>" or len(main_node.childD) + 1 < self.main_parser.maxChild:
                        new_node = self.deep_copy_node(node)
                        main_node.childD[token] = new_node
                        exclude_tokens.add(token)
                    elif len(main_node.childD) + 1 == self.main_parser.maxChild:
                        new_node = self.deep_copy_node(node)
                        main_node.childD["<*>"] = new_node 

        # 递归合并相同的token
        for token, node in main_node.childD.items():
            if token in exclude_tokens:
                continue
            if token in side_node.childD:
                self.merge_node(node, side_node.childD[token])

    def clear_log_ids(self, node: Node):
        if node is None:
            return
        if not isinstance(node.childD, dict):
            for cluster in node.childD:
                cluster.logIDL.clear()
        else:
            for _, child in node.childD.items():
                self.clear_log_ids(child)


    def merge_parser(self, main_parser: LogParser, side_parser: LogParser) -> LogParser:
        main_node = main_parser.root
        side_node = side_parser.root

        # 先按 长度进行合并
        exclude_sizes = set()
        for size, node in side_node.childD.items():
            if size not in main_node.childD:
                main_node.childD[size] = self.deep_copy_node(node)
                exclude_sizes.add(size)
        
        # 再遍历节点进行合并
        for size, node in main_node.childD.items():
            if size not in exclude_sizes and size in side_node.childD:
                self.merge_node(node, side_node.childD[size])
        return main_parser

    
    def merge(self, parsers: List[LogParser], indir, outdir) -> LogParser:
            if len(parsers) == 0:
                raise ValueError("can not parse a empty list of parsers")

            main_parser = parsers[0]
            self.main_parser = main_parser

            for i in range(1, len(parsers)):
                main_parser = self.merge_parser(main_parser, parsers[i])        
            main_parser.path = indir
            main_parser.savePath = outdir
            for node in self.main_parser.root.childD.values():
                self.clear_log_ids(node)
            return main_parser
    
