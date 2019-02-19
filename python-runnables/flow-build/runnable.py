# ______________________________________________________________________________
# Imports 

import numpy as np

import dataiku

from dataiku.runnables import Runnable, ResultTable

import benchmark
import html_template 


class MyRunnable(Runnable):
    """The base interface for a Python runnable"""

    def __init__(self, project_key, config, plugin_config):
        """
        :param project_key: the project in which the runnable executes
        :param config: the dict of the configuration of the object
        :param plugin_config: contains the plugin settings
        """
        self.project_key = project_key
        self.config = config
        self.plugin_config = plugin_config
        
        self.formatType     = self.config.get('formatType') # TO DO: Failure / defaults?
        self.connectionType = self.config.get('connectionType')
        self.sparkPipeline = self.config.get('sparkPipeline')
#         self.sparkPipeline  = True if self.config.get('sparkPipeline') == "True" else False # config.get('sparkPipeline') is a string sent from HTML checkbox value
        self.computeEngine  = self.config.get('computeEngine').upper()
        self.s3Bucket = self.config.get('s3Bucket')
        self.dontEdit = self.config.get('dontEdit')
        self.numRuns = int(self.config.get('numRuns'))
        
    def bad_config(self):
        """
        TO DO: Limit configuration options. Dataiku does this internally.
        So replicate?
        
        
        A function to prevent failed builds.
        
        We check for incompatible file formats and compute engines and 
        warn user if their setup will fail.
        
        For example, if the data format is `postgres`, we
        are using data stored in database. As a result, an engine
        like SPARK will not work.
        
        Therefore, 

        (self.config['formatType'], self.config['engineType'])

        =
        
        ('postgres', 'SPARK')
        
        Will lead to our run to fail to execute. Instead, we provide the user
        with a friendly reminder to try again.
        
        TO DO:
        - Consider all possibilities and edit accordingly.

        """
        set_combination = (self.config['formatType'], self.config['computeEngine'])
        bad_combinations = [
            ('postgres', 'SPARK'),
            ('postgres', 'HIVE'),
        ]
        if set_combination in bad_combinations:
            return True
        
    def run(self, progress_callback, dontEdit=False):
        if self.bad_config():
            return 'Configuration settings impossible - try another combination.'
        
        all_flow_results = []
        
        dontEdit = self.dontEdit
        cf = benchmark.checkpoint_flow(project_key = self.project_key)
        if dontEdit:
            # TO DO -- mini function to run n flows.
            for i in range(self.numRuns):
                flow_results = cf.build_flow()
                all_flow_results.append(flow_results)
        else:    
            cf.set_spark_pipelinability(self.sparkPipeline)
            cf.reformat_flow(self.formatType, self.connectionType, self.s3Bucket)
            cf.set_compute_engines(self.computeEngine)
            
            for i in range(self.numRuns):
                flow_results = cf.build_flow()
                all_flow_results.append(flow_results)
#             flow_results = cf.build_flow()

        res = html_template.res
        # TO (NOT?) DO: Make string formatting nicer.
        res += """
                    <table>
                      <tr>
                        <th> File type </th>
                        <th> Connection type</th>
                        <th> Engine type</th>
                        <th> Spark pipeline? </th>
                      </tr>
                      <tr>
                        <td> {} </td>
                        <td> {} </td>
                        <td> {} </td>
                        <td> {} </td>
                      </tr>""".format(self.formatType, self.connectionType, self.computeEngine, self.sparkPipeline)
        
        def html_row(data_list):
            row = """<tr>
                    <td> {} </td>
                    <td> {} </td>
                </tr>""".format(data_list[0], data_list[1])
            return row
        
        res += html_template.table
        # TO DO: Make plurals less confusing.
        total_build_time_sum = np.nansum((0))
        for flow_results in all_flow_results:
            for i, _ in enumerate(flow_results): 
                res += html_row([flow_results.keys()[i], flow_results.values()[i]])
                total_build_time_sum = np.nansum((total_build_time_sum, flow_results.values()[i]))
        res+="""
        </table>
        """
        res+= """<h2> Average time for all builds: {} </h2>
        </body>
        </html>""".format(total_build_time_sum / len(all_flow_results))
        return res

