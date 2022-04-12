import os 
import logging
import time
from azure.mgmt.datafactory import DataFactoryManagementClient
# from azure.identity import ClientSecretCredential
from azure.common.credentials import ServicePrincipalCredentials
from azure.core.exceptions import HttpResponseError

def trigger_pipeline():
        AZURE_STORAGE_SUBSCRIPTION_ID="add1863e-573d-485e-b299-ff8e8d020cee"
        AZURE_SUBSCRIPTION_ID="add1863e-573d-485e-b299-ff8e8d020cee"
        AZURE_RESOURCE_GROUP_NAME="PragmaRG2"
        AZURE_RESOURCE_GROUP_ID="add1863e-573d-485e-b299-ff8e8d020cee"
        AZURE_CLIENT_ID="66519f8e-d674-4d11-b76f-34c7f57af5cf"
        AZURE_CLIENT_SECRET="jw57Q~Ry2654Il3WjqzopPXIUI4XRSoznPkvp"
        AZURE_ADF_NAME="bigspark-pragma"
        AZURE_ADF_PIPELINE_NAME="pragma_run"
        AZURE_TENANT_ID="75883507-2a40-4498-958c-2456fcc1147e"
    # try:
        print(AZURE_TENANT_ID)
        credentials = ServicePrincipalCredentials(
            AZURE_CLIENT_ID,
            AZURE_CLIENT_SECRET    
        )
        adf = DataFactoryManagementClient(credentials, AZURE_SUBSCRIPTION_ID, AZURE_STORAGE_SUBSCRIPTION_ID)
        pipe_parameters = {
            'dir_name': "pragma-sharepoint",
            'collection_name': "footfall"
        }
        # _logger.info(f"calling ADF pipeline params : {pipe_parameters}")
        # logging.info(f"calling ADF pipeline params : {pipe_parameters}")
        adf_run = adf.pipelines.create_run(resource_group_name=AZURE_RESOURCE_GROUP_NAME,
                                           factory_name=AZURE_ADF_NAME,
                                           pipeline_name=AZURE_ADF_PIPELINE_NAME,
                                           parameters=pipe_parameters)
        inf = True
        while inf:
            p_run = adf.pipeline_runs.get(AZURE_RESOURCE_GROUP_NAME, AZURE_ADF_NAME, adf_run)
            # _logger.info(f"Pipeline Status : {p_run.status}")
            if p_run.status == "Succeeded":
                inf = False
                # _logger.info("Pipeline Execution completed successfully")
    # except HttpResponseError as Error:
    #     if Error.status_code == 429:
    #         print("too many")
    #         # _logger.error("Too many request in ADF")
    #     else:
    # #         raise Exception(Error.message)
    # except Exception as error:
    #     print(f"Error in ADF | utils | trigger_pipeline {error}")
    #     # _logger.error(f"Error in ADF | utils | trigger_pipeline {error}")

trigger_pipeline()
