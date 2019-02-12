# _____________________________________________________________________________
# Imports

import json
import pprint
import re
import time
        
import numpy as np

import dataiku

# import new_definitions
import dataset_defs

import logging

# _____________________________________________________________________________
# Setup

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# _____________________________________________________________________________
# Global variables.

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

# _____________________________________________________________________________
# Checkpoint flow class.

class checkpoint_flow(object):
    
    def __init__(self, project_key = None):
        self.project_key   = project_key
        self.client        = dataiku.api_client()
        self.project       = self.client.get_project(self.project_key)

    def list_source_datasets(self):
        """ List datasets which are the start of flows. 

        These are the opposite of what I later refer to as 'terminal'
        datasets, in some sense.
        """
        source_datasets = []
        dataset_names = [i['name'] for i in self.project.list_datasets()]
        for dataset_name in dataset_names:
            if 'RECIPE_OUTPUT' not in [usage['type'] for usage in self.project.get_dataset(dataset_name).get_usages()]:
                source_datasets.append(dataset_name)
        return source_datasets 

    def set_file_format(self,
                        dataset_name, 
                        formatType,
                        connectionType,
                        s3Bucket = None):
        """ Given a dataset `dataset_name`:
        - change the format to formatType
        - change the connection to connectionType
        
        Args:
        
        dataset_name (str)   : Dataset to operate on in this project.
        formatType   (str)   : Potential options for new file format for HDFS, local. 
        connectionType (str) : Potential dataset connection.
        s3Bucket (str)       : Bucket key if connecting to S3.
        
        (If this dataset is stored in SQL, this function is N/A, as there are not multiple
        file formats.)
            - 'csv'
            - 'parquet'
            - 'avro'
            - 'ORC

        Returns:
            bool: The return value. True for success, False otherwise.
        """
        # Don't touch if it is an input dataset
        def _check_source_data():
            if 'RECIPE_OUTPUT' not in [usage['type'] for usage in self.project.get_dataset(dataset_name).get_usages()]:
                return True
        if _check_source_data():
            return
        
        # Get the dataset's JSON definition.
        try:
            dataset_def = self.project.get_dataset(dataset_name).get_definition()
        except: # Datasets that are exposed from other projects mysteriously don't have a JSON definition available.
            return
        
        # Don't change anything if this was an uploaded file.
        # This should be redundant given `_check_source_data` above.
        if dataset_def['type'] == 'UploadedFiles':
            print('Do not change type of "{}" as this file was uploaded.'.format(dataset_name))
            return

        changed = dataset_def.copy()
        
        """Essential definition keys to change in this JSON are:
               * formatParams
               * formatType
               * type
               * params
        
        ConnectionType can be named by user, so rely on string search
        to map user connection type to valid backend data connection type.
        
        ### TO DO!!! ###
        This is fragile. Maybe I make a connection called Hadoop
        or HadoopDFS...my string match won't catch this and I will
        botch the mapping.
        """
        initialConnectionType = connectionType
        if 'azure' in connectionType.lower():
            connectionType = 'Azure'
        if len([s for s in ['adls', 'wasb', 'hdfs'] if s.lower() in connectionType.lower()]) > 0:
            connectionType = 'HDFS'
        elif 'file_system' in connectionType.lower() or 'filesystem' in connectionType.lower():
            connectionType = 'Filesystem'
        elif 'sql' in connectionType.lower():
            connectionType = 'SQL'
        elif 's3' in connectionType.lower():
            connectionType= 'S3'
        
        """
        Get the formatParams from my dictionary. 
        `dataset_defs.py` contains Python dictionaries which contain
        appropriate content to populate Dataiku dataset JSON definitions
        according to a new definition.
        """
        formatParams = dataset_defs.formatParams[connectionType][formatType]
        # Set the new formatParams key in the dataset definition JSON
        # called `changed`.
        changed['formatParams'] = formatParams
        changed['formatType'] = formatType
        changed['params'] = dataset_defs.params[connectionType]
            
        # Exceptions / particular cases.
        if connectionType == 'SQL': # No formatParams for SQL connections.
            del changed['formatParams']
            del changed['formatType']
            changed['params']['table'] = '${projectKey}_postgres-10.'+dataset_name
            changed['smartName'] = dataset_name
        if connectionType in ["Filesystem", "HDFS"]: # Include path for a dataset on filesystem. HDFS untested.
            changed['params']['path'] = '${projectKey}/' + dataset_name
            changed['params']['connection'] = initialConnectionType
            changed['hiveTableName'] = '${projectKey}' + dataset_name

        if connectionType == "S3":
            changed['params']['bucket'] = s3Bucket
            changed['params']['path'] = '/dataiku/${projectKey}/' + dataset_name
        if connectionType =='Azure':
            changed['params']['path'] = '/${projectKey}/' + dataset_name
        
        """
        ### TO DO!!! ###
        These types are a mandatory part of a dataset definition JSON,
        but it is unclear to me what all possible options are.
        """
        if connectionType == "Filesystem":
            changed['type']= 'Filesystem'
        elif connectionType == 'HDFS': 
            changed['type'] = 'HDFS'
        elif connectionType == 'SQL':
            changed['type'] = 'PostgreSQL'
        elif connectionType == 'S3':
            changed['type'] = 'S3'
        elif connectionType == 'Azure':  # UNTESTED!
            changed['type'] = 'Azure'
            
        self.project.get_dataset(dataset_name).set_definition(changed)
        print('Dataset definition changed. Need to clear data and rebuild. Call `build_dataset()`.')
        return
    
    def set_compute_engine(self,
                           recipe_name,
                           recipe_type,
                           compute_type):
        """ Args:
        
        recipe_name  (str) : Recipe to operate alter compute engine on, if possible.
            - 'sync
            - 'sampling'
            - 'shaker'
            - ...
            
        compute_type (str) : Potential options for new compute type. 
        Given by recipe definition.

        Returns:
            bool: The return value. True for success, False otherwise.
        """
        print("Trying to set engine for {}.".format(recipe_name))
        recipe          = self.project.get_recipe(recipe_name)
        recipe_def      = recipe.get_definition_and_payload()
        recipe_def_json = recipe_def.get_recipe_raw_definition()
        
        # _____________________________________________________________________
        # Ban sync recipe, as this seems to not work?
        if recipe_type == 'sync':
            print("Cannot change engine on sync?")
            return

        # _____________________________________________________________________
        # Ban incompatible compute engines on first recipes.
        # !!!!!UNTESTED!!!!!
        
        # If recipe is first after source data, don't use set engine.
        # For example, don't use HIVE or SQL when input is file system csv.
        recipe_input_datsets = [i['ref'] for i in recipe_def_json['inputs']['main']['items']]
        source_datasets = self.list_source_datasets()
        print("Inputs are: ", recipe_input_datsets)
        source_datasets = self.list_source_datasets()
        print("Project source datasets are: ", source_datasets)

        if set(recipe_input_datsets)&set(source_datasets): # Check intersection
            if compute_type in ['SQL', 'HIVE']:
                compute_type = "DSS"
        
        # _____________________________________________________________________
        # Set new recipe definition
        if recipe_type == 'shaker': # Special case to catch shaker.
            recipe_def_json['params']['engineType'] = compute_type
            recipe_payload = recipe_def.get_json_payload()
            recipe_def.set_json_payload(recipe_payload)
            recipe.set_definition_and_payload(recipe_def)

        else:
            try: # If recipe_def has json payload.
                print('Recipe type for json_payload access is: {}.'.format(recipe_type))
                recipe_payload = recipe_def.get_json_payload()
                recipe_payload['engineType'] = compute_type
                recipe_def.set_json_payload(recipe_payload)
                recipe.set_definition_and_payload(recipe_def)

            except:
                print('Recipe type for setting recipe_def_json is: {}.'.format(recipe_type))
                try:
                    recipe_def_json['params']['engineType'] = compute_type
                except KeyError:
                    recipe_def_json['params'] = {}
                    recipe_def_json['params']['engineType'] = compute_type

                try:  # E.g. prepare recipe
                    recipe_payload = recipe_def.get_json_payload()
                    recipe_def.set_json_payload(recipe_payload)
                    recipe.set_definition_and_payload(recipe_def)
                except ValueError:  # E.g. SparkR recipes don't have straight json_payload.
                    recipe_payload = recipe.get_definition_and_payload()
                    recipe_def.set_payload(recipe_payload)
                    recipe.set_definition_and_payload(recipe_def.get_payload())

        return 

    def list_dataset_names(self):
        """ Helper function.
        
        returns:
        names (list)
        """
        names = []
        for item in self.client.get_project(self.project_key).list_datasets():
            names.append(item['name'])
        print("Datasets are {}.".format(names))
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
        print("Recipes and types are {}.".format(zip(names, recipe_types)))
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
        settings = self.client.get_project(self.project_key).get_settings()
        raw_settings = settings.get_raw()
        raw_settings['settings']['flowBuildSettings']['mergeSparkPipelines'] = able
        settings.save()
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
        
        # https://doc.dataiku.com/dss/latest/publicapi/client-python/jobs.html
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
        print(names, recipe_types)
        
        messages = {}
        for recipe, recipe_type in zip(names, recipe_types):
            message1 = self.set_compute_engine(recipe, recipe_type, engineType)
            messages[recipe] = message1
        return messages
    
    def reformat_flow(self, 
                      formatType, 
                      connectionType,
                      s3Bucket = None):
        """ Reformat an entire data flow.
        
        Args:
        formatType (str): What file format to flow change to?
        
        formatType (str):
            - 'csv'
            - 'parquet'
            ...
        """
#         if names is None:
        names = self.list_dataset_names()

        for i, dataset in enumerate(names):
            print('Trying to change format type for {}.'.format(dataset))
            print(names)
            self.set_file_format(dataset, formatType, connectionType, s3Bucket)

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
    
