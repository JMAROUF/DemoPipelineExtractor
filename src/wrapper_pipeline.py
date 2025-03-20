from datetime import time

from src.pipeline import Pipeline
from src.types import GroupConfig


class WrapperPipeline:
    def __init__(self, config: GroupConfig):
        self.config=config


    def launch(self):
        for job in self.jobs:
            try:
                startTime=time.time()
                pipeline=Pipeline(job)
                pipeline.run()
                endTime=time.time()
                duration=round(endTime-startTime,1)
                print(f"********************* {job.name} ===> {duration} seconds *************************")
            except Exception as e:
                print(f"le job : {job.name} a échoué, cause => {e}")

        def jobs(self):
            configs=sorted([config for config in self.config.configs if config.enable],key=lambda x: x.order)
            return configs