from dataiku import Dataset
import dataiku
from sets import Set

def do(payload, config, plugin_config, inputs):
    if payload["funtastic"] == "engines":
        client      = dataiku.api_client()
        project = client.get_project(dataiku.default_project_key())
        engines = project.get_settings().get_raw()['metrics']['engineConfig'].keys()

    #     connections = client.list_connections().keys()
        return {'engines': engines}
    
    
    if payload["funtastic"] == "connections":
        client      = dataiku.api_client()

        connections = client.list_connections().keys()
        return {'connections': connections}
