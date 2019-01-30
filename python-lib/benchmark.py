# _____________________________________________________________________________
# Imports

import json
import pprint
import re
import time
        
import numpy as np

import dataiku

import new_definitions
import dataset_defs

import logging

# _____________________________________________________________________________
# Setup

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# _____________________________________________________________________________
# Global variables.

# NEW_DATASET_DEFINITION_DICTIONARIES = {'csv'     : new_definitions.csv_def,
#                                        'parquet' : new_definitions.parquet_def,
#                                        'avro'    : new_definitions.avro_def,
#                                        'postgres': new_definitions.postgres_def}

# _____________________________________________________________________________
# Global functions

def _time_job(job):
    """ Parse job logs and perform regex search.
    Sum times in milliseconds, divide by 1000 to return job
    runtime in seconds.
    
    This is a) hacky and b) maybe an interesting way to do this
    as it allows us to parse smaller components of job run
    if one eventually wanted to build this out.
    """
    return [sum(int(i) for i in re.findall( r"(?<=processed in )\d+(?=ms)" , job.get_log()))][0]/1000.0


# def change_def_dict(to_change_json, 
#                     dataset_name, 
#                     project,
#                     project_key,
#                     filetype):
#     """ Very hacky function to change a DSS dataset defintion
#     dictionary to a new formatType.

#     Get dataset definition (a static JSON) from `new_definitions.py` module. 
    
#     We take a working template for a dataset format and then 
#     replace a few key values and then set this defintion dictionary back to the dataset. 
    
#     Thanks to @JeanYves for this idea.
    
#     WARNING: This will probably break on DSS upgrades. Tested on DSS 5.1.0

#     Later, in another function `build_dataset`, we clear and rebuild the dataset.
#     """
#     new_dict = to_change_json.copy()

#     # Make changes to new dictionary `new_dict`.
#     new_dict['name']       = dataset_name
#     new_dict['projectKey'] = project_key
#     new_dict['schema']     = project.get_dataset(dataset_name).get_definition()['schema']
    
#     if filetype in ['csv', 'avro', 'parquet']:
#         new_dict['smartName']               = dataset_name
#         new_dict['params']['path']          = "/${projectKey}/" + dataset_name
#         new_dict['params']['hiveTableName'] = "${projectKey}_"  + dataset_name
        
#     elif filetype == 'postgres':
#         new_dict['schema']                  = project.get_dataset(dataset_name).get_definition()['schema']
#         new_dict['params']['table']         = "${projectKey}_" + dataset_name
#     # TO DO
#     # Change lastModifiedBy, creationTag. Ensure that we are not making 
#     # any changes which will cause builds to break. E.g. are there configuration
#     # parameters set in `new_defintions.py` that we need to be aware of / handle?

#     return new_dict

# _____________________________________________________________________________
# Checkpoint flow class.

class checkpoint_flow(object):
    
    def __init__(self, project_key = None):
        self.project_key = project_key
        self.client      = dataiku.api_client()
        self.project     = self.client.get_project(self.project_key)
        
#     def set_file_format(self,
#                         dataset_name, 
#                         connection_type, 
#                         new_format):
    def set_file_format(self,
                        dataset_name, 
                        new_format,
                        connection_type):
                        # initial_format = None):
        """ Given a dataset in with an initial format `initial_format`,
        change the format to the new format. 
        
        Args:
        
        dataset_name (str) : Dataset to operate on in this project.
        new_format   (str) : Potential options for new file format for HDFS, local. 
        
        (If this dataset is in SQL, this function is N/A, as there are not multiple
        file formats.)
            - 'csv'
            - 'parquet'
            - 'avro'
            - 'ORC

        Returns:
            bool: The return value. True for success, False otherwise.
        """
        dataset_def = self.project.get_dataset(dataset_name).get_definition()
        if dataset_def['type'] == 'UploadedFiles':
            print('Do not change type of "{}" as this file was uploaded.'.format(dataset_name))
            return

        changed = dataset_def.copy()
        # Essential definition keys to change are:
        #       * formatParams
        #       * formatType
        #       * type
        #       * params
        print(connection_type)
        print("XXXXXX", '\n\n\n\n\n')
#         print(new_format)
#         print(dataset_defs.formatParams.keys())
#         print(dataset_defs.formatParams['file_system_managed'].keys())

        formatParams = dataset_defs.formatParams[connection_type][new_format]
        changed['formatParams'] = formatParams
        changed['formatType'] = new_format

        if connection_type == 'sql': 
            del changed['formatParams']  # No formatParams for SQL
            del changed['formatType']
            
            
        changed['params'] = dataset_defs.params[connection_type]

        if connection_type == "file_system_managed":
            changed['type']= 'Filesystem'
        elif connection_type == 'hdfs': 
            changed['type'] = 'HDFS'
        elif connection_type == 'sql':
            changed['type'] = 'PostgreSQL'
            
        # to_change_json = json.loads(NEW_DATASET_DEFINITION_DICTIONARIES[new_format])
        # changed = change_def_dict(to_change_json, 
        #                           dataset_name, 
        #                           self.project,
        #                           self.project_key,
        #                           new_format)
        self.project.get_dataset(dataset_name).set_definition(changed)
        print('Dataset definition changed. Need to clear data and rebuild. Call `build_dataset()`.')
        return
    
    def set_compute_engine(self,
                        recipe_name,
                        recipe_type,
                        compute_type):
        """
        
        Args:
        
        recipe_name (str) : Recipe to operate alter compute engine on, if possible.
        compute_type   (str) : Potential options for new compute type. 
        
            - 'DSS'
            - 'SPARK'
            - 'HIVE'

        Returns:
            bool: The return value. True for success, False otherwise.
        """
        print(compute_type)
        rdp = self.project.get_recipe(recipe_name).get_definition_and_payload()
        # Hacky way to ensure that a user compute engine does not get set
        
        recipe_raw_def = rdp.get_recipe_raw_definition()
        inputs = recipe_raw_def['inputs']['main']['items']
#         check type of input dataset
#        do some logic here so that we dont set inappropriate compute engine
        input_file_types = []
        for input in inputs:
            ds = self.project.get_dataset(input['ref'])
            r_type = ds.get_definition()['type']
            input_file_types.append(r_type)
            
            
        if compute_type == 'HIVE' and 'postgres-10' in input_file_types:
            print('Incompitable. Can not set {0} compute engine with {1} file type.'.format(compute_type, input_file_types))
        # if incompatible with recipe type.
        if recipe_type in ['sync']:
            # Don't allow a sync recipe to be set to SQL.
            if compute_type == 'sql':
                new_compute_type = 'dss'
            else:
                new_compute_type = compute_type
            raw_def = rdp.get_recipe_raw_definition()
            raw_def['params']['engineType'] = new_compute_type
            rdp.set_json_payload(raw_def)
            return self.project.get_recipe(recipe_name).set_definition_and_payload(rdp)['msg']
        
        elif recipe_type == 'shaker':
            # Keep payload
            json_payload= rdp.get_json_payload()

            # Change compute type 
            rdp.get_recipe_raw_definition()['params']['engineType'] = compute_type

            # Explicitly retain JSON payload
            rdp.set_json_payload(json_payload)

            # Set back json payload and new defition.
            return self.project.get_recipe(recipe_name).set_definition_and_payload(rdp)['msg']

        elif recipe_type in ['distinct',
                             'group',
                             'join',
                             'pivot'
                             'sort',
                             'split',
                             'stack',
                             'topn',
                             'window']:  # TO DO: Check to make sure all SQL recipes are as so.
            jso = rdp.get_json_payload()
            jso['engineType'] = compute_type
            rdp.set_json_payload(jso)
            return self.project.get_recipe(recipe_name).set_definition_and_payload(rdp)['msg']       

    def list_dataset_names(self):
        """ Helper function.
        
        returns:
        names (list)
        """
        names = []
        for item in self.client.get_project(self.project_key).list_datasets():
            names.append(item['name'])
        return names
    
    def list_recipe_names(self):
        """ Helper function.
        
        returns:
        names (list)
        recipe_types (list)
        """
        names = []
        recipe_types = []
        for item in self.client.get_project(self.project_key).list_recipes():
            names.append(item['name'])
            recipe_types.append(item['type'])
        return names, recipe_types
    
    def set_spark_pipelinability(self,
                                able = False):
        """ Set flag in project settings to allow Spark pipelines.
        
        Default is false, and this is turned on by selecting checkbox
        in macro launch GUI.
        
        Stupid question -- will we be able to leverage
        Spark pipelines when we build datasets one by one
        as we do in `build_flow`?
        """
        print("XXXXXXX \n\n\n\n before able")
        print(able)
        s = self.client.get_project(self.project_key).get_settings()
        r = s.get_raw()
        r['settings']['flowBuildSettings']['mergeSparkPipelines'] = able
        s.save()
        return 
        
    def build_dataset(self,
                      output_dataset_name, 
                      clear_before_build = True,
                      verbose = True):
        """
        
        Clear a dataset and rebuild according to whatever defintion is currently applied.
        
        Note, this is theoretically equivalent to running a recipe.
        
        See this discussion for preference over "job execution" versus 
        "recipe running": https://answers.dataiku.com/3687/run-a-recipe-using-dataiku-api
        
        Args:
        output_dataset_name (str)
        clear_before_build  (bool):
            - Must be set to true to clear data if we plan to change format type through API.
        verbose (bool):
            - Set print statements.
            
        Returns:
            bool: The return value. True for success, False otherwise.
        """
        if clear_before_build:
            if self.project.get_dataset(output_dataset_name).get_definition()['type'] != 'UploadedFiles':
                self.project.get_dataset(dataset_name=output_dataset_name).clear()
                print('Dataset {} cleared.'.format(output_dataset_name))
        
#         https://doc.dataiku.com/dss/latest/publicapi/client-python/jobs.html
        definition = {
            "type" : "RECURSIVE_FORCED_BUILD",  # Run recursive forced build to build entire preceding flow.
            "refreshHiveMetastore" : True, # ? Will this help my issue?
            "outputs" : [{"id" : output_dataset_name,
                          "partition" : "NP"}]
                     }
        job = self.project.start_job(definition)
        state = ''
        while state != 'DONE' and state != 'FAILED' and state != 'ABORTED':
            time.sleep(1)
            state = job.get_status()['baseStatus']['state']
            if state == 'FAILED' or state=='ABORTED':
                print('Dataset build failed :(.')
                return False
            if verbose == True:
                print(state)
                
        return job
    
    def set_compute_engines(self, 
                      engineType, 
                      names = None,
                      recipe_types = None,
                      verbose = True):
        if names is None:
            names, recipe_types = self.list_recipe_names()

        messages = {}
        for recipe, recipe_type in zip(names, recipe_types):
            message1 = self.set_compute_engine(recipe, recipe_type, engineType)
            messages[recipe] = message1
        return messages
    
    def reformat_flow(self, 
                      formatType, 
                      connectionType,
                      names = None,
                      verbose = True):
        """ Reformat an entire data flow.
        
        Args:
        formatType (str): What file format to flow change to?
        
        formatType (str):
            - 'csv'
            - 'parquet'
            ...
        """
        if names is None:
            names = self.list_dataset_names()

        for i, dataset in enumerate(names):
            print('Trying to change connection type for {}.'.format(dataset))
            self.set_file_format(dataset, formatType, connectionType)

    def build_flow(self, 
                   names = None):
        """ Build entire data flow.
        
        Find all 'terminal' datasets...e.g. datasets which are not
        the inputs to any other datasets.
        
        Build all of these to mimic the 'flow' build.
        """
        if names is None:
            names = self.list_dataset_names()
            logger.info("Dataset names are: {}".format(names))
            
        terminal_datasets = []
        for dataset in names:
            if 'RECIPE_INPUT' not in [usage['type'] for usage in self.project.get_dataset(dataset).get_usages()]:
                terminal_datasets.append(dataset)
        
        run_times = {}
        for terminal_dataset in terminal_datasets:
            job = self.build_dataset(terminal_dataset)
            if not job:  # Don't error out of loop if we fail one job.
                run_times[terminal_dataset] = np.nan
            else:
                run_times[terminal_dataset] = _time_job(job)
                print('Successfully built {}.'.format(dataset))
           
        return run_times
#     'Run times were {} and total run time was {} seconds.'.format(run_times, sum(run_times.values()))
    
        
