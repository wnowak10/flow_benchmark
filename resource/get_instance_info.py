import dataiku

def do(payload, config, plugin_config, inputs):
    
    client = dataiku.api_client()
    
    if payload["funtastic"] == "projects":
        project_list = client.list_project_keys()
    
        return{'projects' : project_list}