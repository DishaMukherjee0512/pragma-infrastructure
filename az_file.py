import os
from checkkk import *
import logging

_logger = logging.getLogger(__name__)


class AzPipelineCall(object):
    def __init__(self):
        # self.v={}
        # self.v={
        #     "variable1" : os.environ['p-id'],
        #     "variable2": os.environ['v1'],
        #     "variable3": os.environ['v2'],
        #     "variable4": os.environ['v3'],
        #     "variable5": os.environ['v4']
        #
        # }
        # self.p_id = os.environ['p_id'],
        # self.dir_name = os.environ['dir_name'],
        # self.collection_name = os.environ['collection_name']
        self.p_id = os.getpid()
        self.dir_name = "pragma-sharepoint"
        self.collection_name = "footfall"


        log_file = set_log(f"Pragma_{self.p_id}_{self.dir_name}")
        _logger.info("_________________________________________________________________")
        # _logger.info("Log File Name : ", log_file)
        _logger.info("_________________________________________________________________")
        print("Log File Name : ", log_file)
        # _logger.info("Process Id : ", self.p_id)
        # _logger.info("Directory Name : ", self.dir_name)
        # _logger.info("Collection Name : ", self.collection_name)


    def process(self):
        trigger_pipeline(self)

if __name__ == "__main__":
    s_time = time.time()
    print("-----------------------------------Process Begins-------------------------")
    try:
        _logger.info("---------------------------------Process Begins---------------------------------")
        cls_obj = AzPipelineCall()
        cls_obj.process()
        _logger.info("---------------------------------Process Completed---------------------------------")
    except Exception as Error:
        _logger.error("Error in main function : ", Error )
        _logger.error("see logs for more details")
        _logger.info("Time Taken : ", time.time() - s_time)

    print("Time Taken : ", time.time() - s_time)
    print("-----------------------------------Process Ended-------------------------")
