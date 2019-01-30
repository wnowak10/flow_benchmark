from dataiku import Dataset
import dataiku
from sets import Set

# paylaod is sent from the javascript's callPythonDo()
# config and plugin_config are the recipe/dataset and plugin configured values
# inputs is the list of input roles (in case of a recipe)
def do(payload, config, plugin_config, inputs):
    client      = dataiku.api_client()
    project = client.get_project(dataiku.default_project_key())
    engines = project.get_settings().get_raw()['metrics']['engineConfig'].keys()
    
#     connections = client.list_connections().keys()
    return {'choices': engines}
