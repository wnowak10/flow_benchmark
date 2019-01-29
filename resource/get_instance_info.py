import dataiku

def do(payload, config, plugin_config, inputs):
    
    client = dataiku.api_client()
    pkey = dataiku.get_custom_variables()['projectKey']
    project = client.get_project(pkey)
    
    if payload["funtastic"] == "datasets":
        dataset_list = project.list_datasets()
    
        return{'datasets' : dataset_list}
    
#     if payload["funtastic"] == "folders":
#         current_proj_key = dataiku.get_custom_variables()['projectKey']
#         proj = client.get_project(current_proj_key)
#         folder_names = []
#         folders = proj.list_managed_folders()
#         for folder in folders:
#             folder_names.append(folder['name'])
            
#         return{'folders' : folder_names}
