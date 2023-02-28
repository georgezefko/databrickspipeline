from dotenv import load_dotenv
import dbutils
import os
# load variables from .env file
load_dotenv('/Workspace/Repos/s151074@dtu.dk/databrickspipeline/.env')

# get variable values
account = os.getenv("ACCOUNT")
blob = os.getenv("BLOB")
key = os.getenv("KEY")
scope = os.getenv("SCOPE")

def mount(blob_account_name,blob_container_name,scope,key):
    
    blob_access_key = dbutils.secrets.get(scope=scope,key=key)

    # already mounted
    dbutils.fs.mount(
    source="wasbs://"+blob_container_name+"@"+blob_account_name+".blob.core.windows.net",
    mount_point="/mnt/myblob",
    extra_configs={
        "fs.azure.account.key."+blob_account_name+".blob.core.windows.net": blob_access_key
    }
    )