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
        self.project_key = project_key
        self.client      = dataiku.api_client()
        self.project     = self.client.get_project(self.project_key)
        
    def set_file_format(self,
                        dataset_name, 
                        formatType,
                        connectionType):
        """ Given a dataset `dataset_name`:
        - change the format to formatType
        - change the connection to connectionType
        
        Args:
        
        dataset_name (str)  : Dataset to operate on in this project.
        formatType   (str)  : Potential options for new file format for HDFS, local. 
        connectionType (str) : Potential dataset connection.
        
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
        if 'RECIPE_OUTPUT' not in [usage['type'] for usage in self.project.get_dataset(dataset_name).get_usages()]:
            return
        
        # Get the dataset's JSON definition.
        try:
            dataset_def = self.project.get_dataset(dataset_name).get_definition()
        except: # Datasets that are exposed from other projects mysteriously don't have a JSON definition available.
            return
        
        # Don't change anything if this was an uploaded file.
        if dataset_def['type'] == 'UploadedFiles':
            print('Do not change type of "{}" as this file was uploaded.'.format(dataset_name))
            return
        changed = dataset_def.copy()
        
        """Essential definition keys to change in this JSON are:
               * formatParams
               * formatType
               * type
               * params
        """
        
        """
        ConnectionType can be named by user, so rely on string search
        to map user connection type to valid backend data connection type.
        
        ### TO DO!!! ###
        This is fragile. Maybe I make a connection called Hadoop
        or HadoopDFS...my string match won't catch this and I will
        botch the mapping.
        """
        
        if 'hdfs' in connectionType.lower():
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
            changed['params']['path'] = '${projectKey}/'+dataset_name
        if connectionType == "S3":
            changed['params']['path'] = '${projectKey}.'+dataset_name  
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
            
        self.project.get_dataset(dataset_name).set_definition(changed)
        print('Dataset definition changed. Need to clear data and rebuild. Call `build_dataset()`.')
        return
    
    def set_compute_engine(self,
                        recipe_name,
                        recipe_type,
                        compute_type):
        """
        
        Args:
        
        recipe_name  (str) : Recipe to operate alter compute engine on, if possible.
            - 'sync
            - 'sampling'
            - 'shaker'
            - 
            
        compute_type (str) : Potential options for new compute type. 
            - 'DSS'
            - 'SQL
            - 'SPARK'
            - 'HIVE'

        Returns:
            bool: The return value. True for success, False otherwise.
        """
        print("Trying to set engine for {}.".format(recipe_name))
        rdp = self.project.get_recipe(recipe_name).get_definition_and_payload()
        
        # Hacky way to ensure that a user compute engine does not get set
        # inappropriately.
        recipe_raw_def = rdp.get_recipe_raw_definition()
        print("Loaded recipe_raw_definition.")
        input_datasets = recipe_raw_def['inputs']['main']['items']
        
        input_file_types = []
        for input in input_datasets:
            ds = self.project.get_dataset(input['ref'])
            # Check file type of recipe's input dataset.
            try:
                r_type = ds.get_definition()['type']
            except:
                r_type = 'shaker' # Or sync? # Missing definition when using an exposed dataset.
            input_file_types.append(r_type)
        print("Loaded input file types. They are {}.".format(input_file_types))
        # Very hacky logic to prevent bad combinations of computeType and input file types.
        # For example, "HIVE" as computeType will not work with "postgres-10"
        # as a file type for one of the recipe inputs.
        if compute_type != 'HIVE' and 'postgres-10' in input_file_types:
            print('Incompitable. Can not set {0} compute engine with {1} file type.'.format(compute_type, input_file_types))

        # Logic to prevent incompatible computeTypes with various recipe types.
        if recipe_type in ['sync', 'sampling']:
            # Don't allow a sync recipe to be set to SQL.
            if compute_type == 'SQL':
                new_compute_type = 'DSS'
            else:
                new_compute_type = compute_type
            raw_def = rdp.get_recipe_raw_definition()
            raw_def['params']['engineType'] = new_compute_type
            rdp.set_json_payload(raw_def)
            return self.project.get_recipe(recipe_name).set_definition_and_payload(rdp)['msg']
        
        if recipe_type in [ 'python', 'r']:
            if compute_type != 'SPARK':
                compute_type = "DSS"
        # TO DO: Deal w containerization?

        elif recipe_type in ['shaker']:
            # Keep payload
            json_payload= rdp.get_json_payload()

            # Change compute type 
            rdp.get_recipe_raw_definition()['params']['engineType'] = compute_type # compute_type

            # Explicitly retain JSON payload
            rdp.set_json_payload(json_payload)

            # Set back json payload and new defition.
            return self.project.get_recipe(recipe_name).set_definition_and_payload(rdp)['msg']
        
        if recipe_type in ['split']:
            # Don't allow a sync recipe to be set to SQL.
            if compute_type == 'SPARK':
                new_compute_type = 'HIVE'
            else:
                new_compute_type = compute_type
            jso = rdp.get_json_payload()
            jso['engineType'] = new_compute_type
            rdp.set_json_payload(jso)
            return self.project.get_recipe(recipe_name).set_definition_and_payload(rdp)['msg']
        
        # TO DO -- delete. Just test if problem with Nowak Installation Suite Test
        # is that I am using SQL as engine from two file system datasets?
        if recipe_type == 'stack':
            compute_type == 'DSS'
            jso = rdp.get_json_payload()
            jso['engineType'] = compute_type
            rdp.set_json_payload(jso)
            return self.project.get_recipe(recipe_name).set_definition_and_payload(rdp)['msg']
        
        if recipe_type in ['pyspark', 'spark_scala', 'spark_sql_query', 'sparkr']:
            # Don't allow a sync recipe to be set to SQL.
            new_compute_type = compute_type
            if compute_type not in ['SPARK', "DSS"]:
                new_compute_type = 'DSS'
            else:
                new_compute_type = compute_type
            jso = rdp.get_json_payload()
            jso['engineType'] = new_compute_type
            rdp.set_json_payload(jso)
            return self.project.get_recipe(recipe_name).set_definition_and_payload(rdp)['msg']
        
        if recipe_type in ['sparkr']:
            raw_def = rdp.get_definition_and_payload().get_recipe_raw_definition()
            raw_def['params']['engineType'] = "DSS"
            rdp.set_json_payload(raw_def)
            self.project.get_recipe(recipe_name).set_definition_and_payload(rdp)['msg']
        
        elif recipe_type in ['distinct',
                             'grouping',
                             'join',
                             'pivot',
                             'sort',
                             'split',
                             'vstack',
#                              'sampling', # Filter recipe 
                             'topn',
                             'window']:  # TO DO: Check to make sure all SQL recipes are as so.
            jso = rdp.get_json_payload()
            jso['engineType'] = compute_type
            rdp.set_json_payload(jso)
            self.project.get_recipe(recipe_name).set_definition_and_payload(rdp)
            return 

    def list_dataset_names(self):
        """ Helper function.
        
        returns:
        names (list)
        """
        names = []
        for item in self.client.get_project(self.project_key).list_datasets():
            names.append(item['name'])
        print("Recipes are {}.".format(names))
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
    
        
