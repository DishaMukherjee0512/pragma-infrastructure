import os 
import logging
import time
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.identity import ClientSecretCredential
from azure.core.exceptions import HttpResponseError
# from az_file import AzPipelineCall

_logger = logging.getLogger(__name__)


def set_log(prefix: str):
    log_dir = "logs/"
    if log_dir is None:
        raise RuntimeError("Log Dir is not set ")
    curr_time = time.localtime()
    log_file = f"{prefix}_{curr_time.tm_year}-{curr_time.tm_mon}-{curr_time.tm_mday}.{curr_time.tm_hour}" \
               f"{curr_time.tm_min}{curr_time.tm_sec}:"
    formatter = logging.Formatter("%(module)s - %(message)s", datefmt="%Y/%M/%D  %H:%M:%S")
    ch = logging.FileHandler(log_file)
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    logging.getLogger().addHandler(ch)
    logging.getLogger().setLevel(logging.INFO)
    return log_file


def trigger_pipeline(self):
    try:
        self.credentials = ClientSecretCredential(
            self.os.environ["AZURE_TENANT_ID"],
            self.os.environ["AZURE_CLIENT_ID"],
            self.os.environ["AZURE_CLIENT_SECRET"]
        )
        adf = DataFactoryManagementClient(self.crentials, self.subscriptionId, self.storageaccsubscriptionid)
        pipe_parameters = {
            'dir_name': self.dir_name,
            'collection_name': self.collection_name
        }
        _logger.info(f"calling ADF pipeline params : {pipe_parameters}")
        logging.info(f"calling ADF pipeline params : {pipe_parameters}")
        adf_run = adf.pipelines.create_run(resource_group_name=self.os.environ["AZURE_RESOURCE_GROUP_NAME"],
                                           factory_name=self.os.environ["AZURE_ADF_NAME"],
                                           pipeline_name=self.os.environ["AZURE_ADF_PIPELINE_NAME"],
                                           parameters=pipe_parameters)
        inf = True
        while inf:
            p_run = adf.pipeline_runs.get(self.os.environ["AZURE_RESOURCE_GROUP_NAME"], self.os.environ["AZURE_ADF_NAME"], adf_run)
            _logger.info(f"Pipeline Status : {p_run.status}")
            if p_run.status == "Succeeded":
                inf = False
                _logger.info("Pipeline Execution completed successfully")
    except HttpResponseError as Error:
        if Error.status_code == 429:
            _logger.error("Too many request in ADF")
        else:
            raise Exception(Error.message)
    except Exception as error:
        _logger.error(f"Error in ADF | utils | trigger_pipeline {error}")
