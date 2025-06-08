import json, os
import pandas as pd
from pathlib import Path
from datetime import datetime
cwd_ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.environ['KAGGLE_CONFIG_DIR'] = os.path.dirname(os.path.abspath(__file__))

from kaggle.api.kaggle_api_extended import KaggleApi

#connect to kaggle api
kg = KaggleApi()
kg.authenticate()

def downnload_kaggle_dataset(dataset_name,meta_file_name):
    #input agruments:
    # dataset_name (str): source for the datasets to be downloaded
    # meta_file (str): meta_file to store the lastUpdated timestamp details related to the given data source

    #To check last update time from kaggle
    directory = cwd_+"/dataset/" + dataset_name + "/" 
    if not os.path.exists(directory):
        os.makedirs(directory)
    meta_file = directory + meta_file_name
    remote_meta = kg.dataset_list(search = "yasserh/instacart-online-grocery-basket-analysis-dataset")
    remote_updated_at = remote_meta[0]._last_updated
    remote_updated_at = remote_updated_at.strftime("%Y-%m-%d %H:%M:%S")

    #compare the remote updatedtime with the last local stored metadata
    need_download = True
    if os.path.exists(meta_file):
        with open(meta_file, 'r') as f:
            local_meta = json.load(f)
            if local_meta.get("lastUpdated") == remote_updated_at:
                need_download = False
                print(f"No New Update to Downloads in {directory}")

    if need_download:
        print(meta_file)
        print("!~~~~~~\n\n\n\n")
        with open(meta_file,'w') as f:
            json.dump({"lastUpdated":remote_updated_at},f)
        kg.dataset_download_files(dataset = "yasserh/instacart-online-grocery-basket-analysis-dataset", path= directory, unzip=True)

#Reading the datasets
#downnload_kaggle_dataset("instacart-online-grocery-basket-analysis-dataset","instacart_meta_file.json")
downnload_kaggle_dataset("bittlingmayer/amazonreviews","amazonreviews_meta_file.json")
#downnload_kaggle_dataset("retailrocket/ecommerce-dataset","retailrocket_meta_file.json")

#df_product = pd.read_csv('dataset/products.csv', encoding='ISO-8859-1') #window encoding
#print(df_product.head(1).transpose())

