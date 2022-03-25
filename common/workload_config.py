""" Common workload config
    The workload runner is responsible for initializing this object prior to initiating workloads
"""
class WorkloadConfig:
    def __init__(self):
        self.num_databases = 0
        self.num_tables = 0
        self.cluster_helper = None

workload_config = WorkloadConfig
