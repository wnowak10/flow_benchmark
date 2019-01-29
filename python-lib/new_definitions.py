""" To store Dataiku public API csv dataset dictionary.
"""

csv_def = '''
{
   "partitioning":{
      "ignoreNonMatchingFile":false,
      "dimensions":[

      ],
      "considerMissingRequestedPartitionsAsEmpty":false
   },
   "formatType":"csv",
   "managed":true,
   "name":"output_dataset_name34",
   "versionTag":{
      "lastModifiedBy":{
         "login":"admin"
      },
      "lastModifiedOn":1543871490902,
      "versionNumber":1
   },
   "smartName":"output_dataset_name34",
   "customMeta":{
      "kv":{

      }
   },
   "tags":[

   ],
   "readWriteOptions":{
      "defaultReadOrdering":{
         "rules":[

         ],
         "enabled":false
      },
      "writeBuckets":1,
      "preserveOrder":false,
      "forceSingleOutputFile":false
   },
   "creationTag":{
      "lastModifiedBy":{
         "login":"admin"
      },
      "lastModifiedOn":1543871490900,
      "versionNumber":0
   },
   "metrics":{
      "engineConfig":{
         "python":{

         },
         "basic":{

         },
         "dss":{
            "active":true,
            "selection":{
               "targetRatio":0.02,
               "maxReadUncompressedBytes":-1,
               "ordering":{
                  "rules":[

                  ],
                  "enabled":false
               },
               "latestPartitionsN":1,
               "filter":{
                  "distinct":false,
                  "enabled":false
               },
               "withinFirstN":-1,
               "maxRecords":-1,
               "partitionSelectionMethod":"ALL",
               "useMemTable":false,
               "samplingMethod":"FULL"
            }
         },
         "hive":{
            "active":true,
            "extraConf":[

            ]
         },
         "sql":{
            "active":true
         },
         "impala":{
            "active":true
         },
         "padRunsWithMetrics":false
      },
      "displayedState":{
         "metrics":[
            "basic:COUNT_COLUMNS",
            "basic:COUNT_FILES",
            "basic:SIZE",
            "records:COUNT_RECORDS"
         ],
         "columns":[

         ]
      },
      "probes":[
         {
            "enabled":true,
            "meta":{
               "name":"Basic data",
               "level":0
            },
            "type":"basic",
            "configuration":{

            },
            "computeOnBuildMode":"PARTITION"
         },
         {
            "enabled":true,
            "meta":{
               "name":"Record count",
               "level":0
            },
            "type":"records",
            "configuration":{

            },
            "computeOnBuildMode":"NO"
         }
      ]
   },
   "metricsChecks":{
      "runOnBuild":false,
      "checks":[

      ],
      "displayedState":{
         "checks":[

         ]
      }
   },
   "params":{
      "path":"${projectKey}/output_dataset_name34",
      "connection":"filesystem_managed",
      "notReadyIfEmpty":false,
      "filesSelectionRules":{
         "excludeRules":[

         ],
         "explicitFiles":[

         ],
         "mode":"ALL",
         "includeRules":[

         ]
      }
   },
   "flowOptions":{
      "virtualizable":false,
      "rebuildBehavior":"NORMAL",
      "crossProjectBuildBehavior":"DEFAULT"
   },
   "checklists":{
      "checklists":[

      ]
   },
   "schema":{
      "userModified":true,
      "columns":[
         {
            "type":"string",
            "name":"col_0"
         },
         {
            "type":"bigint",
            "name":"count"
         }
      ]
   },
   "type":"Filesystem",
   "checks":[

   ],
   "projectKey":"PERF_BENCHMARK",
   "formatParams":{
      "parseHeaderRow":false,
      "style":"excel",
      "escapeChar":"\\\\",
      "compress":"gz",
      "normalizeDoubles":true,
      "skipRowsAfterHeader":0,
      "hiveSeparators":[
         "\\u0002",
         "\\u0003",
         "\\u0004",
         "\\u0005",
         "\\u0006",
         "\\u0007",
         "\\b"
      ],
      "probableNumberOfRecords":0,
      "charset":"utf8",
      "arrayMapFormat":"json",
      "writeDataTypeMismatchBehavior":"DISCARD_WARNING",
      "separator":"\\t",
      "dateSerializationFormat":"ISO",
      "readAdditionalColumnsBehavior":"INSERT_IN_DATA_WARNING",
      "quoteChar":"\\"",
      "readMissingColumnsBehavior":"DISCARD_SILENT",
      "normalizeBooleans":false,
      "skipRowsBeforeHeader":0,
      "readDataTypeMismatchBehavior":"DISCARD_WARNING",
      "fileReadFailureBehavior":"FAIL"
   }
}
'''

avro_def = '''
{
   "checklists":{
      "checklists":[

      ]
   },
   "checks":[

   ],
   "creationTag":{
      "lastModifiedBy":{
         "login":"lpkronek"
      },
      "lastModifiedOn":1527191267600,
      "versionNumber":0
   },
   "customMeta":{
      "kv":{

      }
   },
   "flowOptions":{
      "crossProjectBuildBehavior":"DEFAULT",
      "rebuildBehavior":"EXPLICIT",
      "virtualizable":false
   },
   "formatParams":{
      "avroCompressionMethod":"SNAPPY",
      "compress":"",
      "representsNullFields":false
   },
   "formatType":"parquet",
   "managed":true,
   "metrics":{
      "displayedState":{
         "columns":[

         ],
         "metrics":[
            "basic:COUNT_COLUMNS",
            "basic:COUNT_FILES",
            "basic:SIZE",
            "records:COUNT_RECORDS"
         ]
      },
      "engineConfig":{
         "basic":{

         },
         "dss":{
            "active":true,
            "selection":{
               "filter":{
                  "distinct":false,
                  "enabled":false
               },
               "latestPartitionsN":1,
               "maxReadUncompressedBytes":-1,
               "maxRecords":-1,
               "ordering":{
                  "enabled":false,
                  "rules":[

                  ]
               },
               "partitionSelectionMethod":"ALL",
               "samplingMethod":"FULL",
               "targetRatio":0.02,
               "useMemTable":false,
               "withinFirstN":-1
            }
         },
         "hive":{
            "active":true,
            "extraConf":[

            ]
         },
         "impala":{
            "active":true
         },
         "padRunsWithMetrics":false,
         "python":{

         },
         "sql":{
            "active":true
         }
      },
      "probes":[
         {
            "computeOnBuildMode":"PARTITION",
            "configuration":{

            },
            "enabled":true,
            "meta":{
               "level":0,
               "name":"Basic data"
            },
            "type":"basic"
         },
         {
            "computeOnBuildMode":"NO",
            "configuration":{

            },
            "enabled":true,
            "meta":{
               "level":0,
               "name":"Record count"
            },
            "type":"records"
         }
      ]
   },
   "metricsChecks":{
      "checks":[

      ],
      "displayedState":{
         "checks":[

         ]
      },
      "runOnBuild":false
   },
   "name":"train_copy",
   "params":{
      "connection":"hdfs_managed",
      "filesSelectionRules":{
         "excludeRules":[

         ],
         "explicitFiles":[

         ],
         "includeRules":[

         ],
         "mode":"ALL"
      },
      "hiveDatabase":"",
      "hiveTableName":"${projectKey}_train_copy",
      "metastoreSynchronizationEnabled":true,
      "notReadyIfEmpty":false,
      "path":"/${projectKey}/train_copy"
   },
   "partitioning":{
      "considerMissingRequestedPartitionsAsEmpty":false,
      "dimensions":[

      ],
      "ignoreNonMatchingFile":false
   },
   "projectKey":"TALKINGDATAKAGGLE",
   "readWriteOptions":{
      "defaultReadOrdering":{
         "enabled":false,
         "rules":[

         ]
      },
      "forceSingleOutputFile":false,
      "preserveOrder":false,
      "writeBuckets":1
   },
   "schema":{
      "columns":[
         {
            "name":"ip",
            "type":"string"
         },
         {
            "name":"app",
            "type":"string"
         },
         {
            "name":"device",
            "type":"string"
         },
         {
            "name":"os",
            "type":"string"
         },
         {
            "name":"channel",
            "type":"string"
         },
         {
            "name":"click_time",
            "type":"string"
         },
         {
            "name":"attributed_time",
            "type":"string"
         },
         {
            "name":"is_attributed",
            "type":"string"
         }
      ],
      "userModified":true
   },
   "tags":[

   ],
   "type":"HDFS",
   "versionTag":{
      "lastModifiedBy":{
         "login":"wnowak"
      },
      "lastModifiedOn":1544641895873,
      "versionNumber":6
   }
}
'''

parquet_def = '''
{
   "checklists":{
      "checklists":[

      ]
   },
   "checks":[

   ],
   "creationTag":{
      "lastModifiedBy":{
         "login":"lpkronek"
      },
      "lastModifiedOn":1527191267600,
      "versionNumber":0
   },
   "customMeta":{
      "kv":{

      }
   },
   "flowOptions":{
      "crossProjectBuildBehavior":"DEFAULT",
      "rebuildBehavior":"EXPLICIT",
      "virtualizable":false
   },
   "formatParams":{
      "parquetBlockSizeMB":128,
      "parquetCompressionMethod":"SNAPPY",
      "parquetFlavor":"HIVE",
      "parquetLowerCaseIdentifiers":false,
      "representsNullFields":false
   },
   "formatType":"parquet",
   "managed":true,
   "metrics":{
      "displayedState":{
         "columns":[

         ],
         "metrics":[
            "basic:COUNT_COLUMNS",
            "basic:COUNT_FILES",
            "basic:SIZE",
            "records:COUNT_RECORDS"
         ]
      },
      "engineConfig":{
         "basic":{

         },
         "dss":{
            "active":true,
            "selection":{
               "filter":{
                  "distinct":false,
                  "enabled":false
               },
               "latestPartitionsN":1,
               "maxReadUncompressedBytes":-1,
               "maxRecords":-1,
               "ordering":{
                  "enabled":false,
                  "rules":[

                  ]
               },
               "partitionSelectionMethod":"ALL",
               "samplingMethod":"FULL",
               "targetRatio":0.02,
               "useMemTable":false,
               "withinFirstN":-1
            }
         },
         "hive":{
            "active":true,
            "extraConf":[

            ]
         },
         "impala":{
            "active":true
         },
         "padRunsWithMetrics":false,
         "python":{

         },
         "sql":{
            "active":true
         }
      },
      "probes":[
         {
            "computeOnBuildMode":"PARTITION",
            "configuration":{

            },
            "enabled":true,
            "meta":{
               "level":0,
               "name":"Basic data"
            },
            "type":"basic"
         },
         {
            "computeOnBuildMode":"NO",
            "configuration":{

            },
            "enabled":true,
            "meta":{
               "level":0,
               "name":"Record count"
            },
            "type":"records"
         }
      ]
   },
   "metricsChecks":{
      "checks":[

      ],
      "displayedState":{
         "checks":[

         ]
      },
      "runOnBuild":false
   },
   "name":"train_copy",
   "params":{
      "connection":"hdfs_managed",
      "filesSelectionRules":{
         "excludeRules":[

         ],
         "explicitFiles":[

         ],
         "includeRules":[

         ],
         "mode":"ALL"
      },
      "hiveDatabase":"",
      "hiveTableName":"${projectKey}_train_copy",
      "metastoreSynchronizationEnabled":true,
      "notReadyIfEmpty":false,
      "path":"/${projectKey}/train_copy"
   },
   "partitioning":{
      "considerMissingRequestedPartitionsAsEmpty":false,
      "dimensions":[

      ],
      "ignoreNonMatchingFile":false
   },
   "projectKey":"TALKINGDATAKAGGLE",
   "readWriteOptions":{
      "defaultReadOrdering":{
         "enabled":false,
         "rules":[

         ]
      },
      "forceSingleOutputFile":false,
      "preserveOrder":false,
      "writeBuckets":1
   },
   "schema":{
      "columns":[
         {
            "name":"ip",
            "type":"string"
         },
         {
            "name":"app",
            "type":"string"
         },
         {
            "name":"device",
            "type":"string"
         },
         {
            "name":"os",
            "type":"string"
         },
         {
            "name":"channel",
            "type":"string"
         },
         {
            "name":"click_time",
            "type":"string"
         },
         {
            "name":"attributed_time",
            "type":"string"
         },
         {
            "name":"is_attributed",
            "type":"string"
         }
      ],
      "userModified":true
   },
   "tags":[

   ],
   "type":"HDFS",
   "versionTag":{
      "lastModifiedBy":{
         "login":"wnowak"
      },
      "lastModifiedOn":1544641895873,
      "versionNumber":6
   }
}
'''

postgres_def = '''
{
  "partitioning": {
    "ignoreNonMatchingFile": false,
    "dimensions": [],
    "considerMissingRequestedPartitionsAsEmpty": false
  },
  "formatType": "csv",
  "managed": true,
  "name": "just_nums",
  "versionTag": {
    "lastModifiedBy": {
      "login": "wnowak"
    },
    "lastModifiedOn": 1545056518833,
    "versionNumber": 10
  },
  "tags": [],
  "customMeta": {
    "kv": {}
  },
  "readWriteOptions": {
    "defaultReadOrdering": {
      "rules": [],
      "enabled": false
    },
    "writeBuckets": 1,
    "preserveOrder": false,
    "forceSingleOutputFile": false
  },
  "creationTag": {
    "lastModifiedBy": {
      "login": "admin"
    },
    "lastModifiedOn": 1543871490900,
    "versionNumber": 0
  },
  "metrics": {
    "engineConfig": {
      "python": {},
      "basic": {},
      "dss": {
        "active": true,
        "selection": {
          "targetRatio": 0.02,
          "maxReadUncompressedBytes": -1,
          "ordering": {
            "rules": [],
            "enabled": false
          },
          "latestPartitionsN": 1,
          "filter": {
            "distinct": false,
            "enabled": false
          },
          "withinFirstN": -1,
          "maxRecords": -1,
          "partitionSelectionMethod": "ALL",
          "useMemTable": false,
          "samplingMethod": "FULL"
        }
      },
      "hive": {
        "active": true,
        "extraConf": []
      },
      "sql": {
        "active": true
      },
      "impala": {
        "active": true
      },
      "padRunsWithMetrics": false
    },
    "displayedState": {
      "metrics": [
        "basic:COUNT_COLUMNS",
        "basic:COUNT_FILES",
        "basic:SIZE",
        "records:COUNT_RECORDS"
      ],
      "columns": []
    },
    "probes": [
      {
        "enabled": true,
        "meta": {
          "name": "Basic data",
          "level": 0
        },
        "type": "basic",
        "configuration": {},
        "computeOnBuildMode": "PARTITION"
      },
      {
        "enabled": true,
        "meta": {
          "name": "Record count",
          "level": 0
        },
        "type": "records",
        "configuration": {},
        "computeOnBuildMode": "NO"
      }
    ]
  },
  "metricsChecks": {
    "runOnBuild": false,
    "checks": [],
    "displayedState": {
      "checks": []
    }
  },
  "params": {
    "writeJDBCBadDataBehavior": "DISCARD_ROW",
    "normalizeDoubles": true,
    "writeInsertBatchSize": 10000,
    "readColsWithUnknownTzAsDates": false,
    "tableCreationMode": "auto",
    "connection": "postgresql-10",
    "mode": "table",
    "table": "${projectKey}_just_nums",
    "notReadyIfEmpty": false,
    "writeWithCopyBadDataBehavior": "NOVERIFY_ERROR",
    "partitioningType": "custom",
    "readSQLDateColsAsDSSDates": true
  },
  "flowOptions": {
    "virtualizable": false,
    "rebuildBehavior": "NORMAL",
    "crossProjectBuildBehavior": "DEFAULT"
  },
  "checklists": {
    "checklists": []
  },
  "schema": {
    "userModified": false,
    "columns": [
      {
        "type": "double",
        "name": "CPIAUCSL"
      }
    ]
  },
  "type": "PostgreSQL",
  "checks": [],
  "projectKey": "NOWAK_SANDBOX",
  "formatParams": {
    "parseHeaderRow": false,
    "style": "excel",
    "escapeChar": "\\\\",
    "compress": "gz",
    "normalizeDoubles": true,
    "skipRowsAfterHeader": 0,
    "hiveSeparators": [
      "\\u0002",
      "\\u0003",
      "\\u0004",
      "\\u0005",
      "\\u0006",
      "\\u0007",
      "\\b"
    ],
    "probableNumberOfRecords": 0,
    "charset": "utf8",
    "arrayMapFormat": "json",
    "writeDataTypeMismatchBehavior": "DISCARD_WARNING",
    "separator": "\\t",
    "dateSerializationFormat": "ISO",
    "readAdditionalColumnsBehavior": "INSERT_IN_DATA_WARNING",
    "quoteChar": "",
    "readMissingColumnsBehavior": "DISCARD_SILENT",
    "normalizeBooleans": false,
    "skipRowsBeforeHeader": 0,
    "readDataTypeMismatchBehavior": "DISCARD_WARNING",
    "fileReadFailureBehavior": "FAIL"
  }
}
'''











