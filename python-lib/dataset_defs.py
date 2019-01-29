formatParams = {	
	"hdfs" :	{
		"parquet" :	{
							'parquetBlockSizeMB': 128,
						  	'parquetCompressionMethod': 'SNAPPY',
							'parquetFlavor': 'HIVE',
							'parquetLowerCaseIdentifiers': False,
							'representsNullFields': False
					},
		"avro" :	{
						'avroCompressionMethod': 'SNAPPY',
						'compress': '',
  						'representsNullFields': False
					},
		"csv" :		{
		    			'arrayItemSeparator': '\x02',
					    'arrayMapFormat': 'hive',
					    'charset': 'utf8',
					    'compress': '',
					    'dateSerializationFormat': 'HIVE',
					    'escapeChar': '\\',
					    'fileReadFailureBehavior': 'FAIL',
					    'hiveSeparators': ['\x02',
					      '\x03',
					      '\x04',
					      '\x05',
					      '\x06',
					      '\x07',
					      '\x08'],
					    'mapKeySeparator': '\x03',
					    'normalizeBooleans': False,
					    'normalizeDoubles': True,
					    'parseHeaderRow': False,
					    'probableNumberOfRecords': 0,
					    'quoteChar': '"',
					    'readAdditionalColumnsBehavior': 'INSERT_IN_DATA_WARNING',
					    'readDataTypeMismatchBehavior': 'DISCARD_WARNING',
					    'readMissingColumnsBehavior': 'DISCARD_SILENT',
					    'separator': '\t',
					    'skipRowsAfterHeader': 0,
					    'skipRowsBeforeHeader': 0,
					    'style': 'escape_only_no_quote',
					    'writeDataTypeMismatchBehavior': 'DISCARD_WARNING'
					}
				},
	"file_system_managed" :	{
		"csv" : 	{
				  'arrayMapFormat': 'json',
				  'charset': 'utf8',
				  'compress': 'gz',
				  'dateSerializationFormat': 'ISO',
				  'escapeChar': '\\',
				  'fileReadFailureBehavior': 'FAIL',
				  'hiveSeparators': ['\x02',
				   '\x03',
				   '\x04',
				   '\x05',
				   '\x06',
				   '\x07',
				   '\x08'],
				  'normalizeBooleans': False,
				  'normalizeDoubles': True,
				  'parseHeaderRow': False,
				  'probableNumberOfRecords': 0,
				  'quoteChar': '"',
				  'readAdditionalColumnsBehavior': 'INSERT_IN_DATA_WARNING',
				  'readDataTypeMismatchBehavior': 'DISCARD_WARNING',
				  'readMissingColumnsBehavior': 'DISCARD_SILENT',
				  'separator': '\t',
				  'skipRowsAfterHeader': 0,
				  'skipRowsBeforeHeader': 0,
				  'style': 'unix',
				  'writeDataTypeMismatchBehavior': 'DISCARD_WARNING'
				  }
		    },
	"sql" :	{
			"postgres" : "None"
			    },
	}

params = {
	"file_system_managed" : {u'connection': u'filesystem_managed',
     u'filesSelectionRules': {u'excludeRules': [],
      u'explicitFiles': [],
      u'includeRules': [],
      u'mode': u'ALL'},
     u'notReadyIfEmpty': False,
     u'path': u'${projectKey}/filesystem_managed_csv'}
    
    
    
    {
      'connection': 'postgresql-10',
	  'mode': 'table',
	  'normalizeDoubles': True,
	  'notReadyIfEmpty': False,
	  'partitioningType': 'custom',
	  'readColsWithUnknownTzAsDates': False,
	  'readSQLDateColsAsDSSDates': True,
	  'table': '${projectKey}_postgres-10',
	  'tableCreationMode': 'auto',
	  'writeInsertBatchSize': 10000,
	  'writeJDBCBadDataBehavior': 'DISCARD_ROW',
	  'writeWithCopyBadDataBehavior': 'NOVERIFY_ERROR'
	  },
	"sql" : {
	  'connection': 'postgresql-10',
	  'mode': 'table',
	  'normalizeDoubles': True,
	  'notReadyIfEmpty': False,
	  'partitioningType': 'custom',
	  'readColsWithUnknownTzAsDates': False,
	  'readSQLDateColsAsDSSDates': True,
	  'table': '${projectKey}_postgres-10',
	  'tableCreationMode': 'auto',
	  'writeInsertBatchSize': 10000,
	  'writeJDBCBadDataBehavior': 'DISCARD_ROW',
	  'writeWithCopyBadDataBehavior': 'NOVERIFY_ERROR'
  },
  	"hdfs" : {
  	  'connection': 'hdfs_managed',
      'filesSelectionRules': {'excludeRules': [],
	  'explicitFiles': [],
	  'includeRules': [],
	  'mode': 'ALL'},
	  'hiveDatabase': '',
	  'hiveTableName': '${projectKey}_hdfs_manged_parquet',
	  'metastoreSynchronizationEnabled': True,
	  'notReadyIfEmpty': False,
	  'path': '/${projectKey}/hdfs_manged_parquet'}
}