
SELECT TOP 1 
      [CONTAINER_PROPERTIES]
	  ,DATALENGTH(CONTAINER_PROPERTIES) AS SizeInBytes
FROM [PLS_MultiTenant_Staging].[dbo].[JOB] order by SizeInBytes desc;

SELECT TOP 1 
      [DATA]
	  ,DATALENGTH(DATA) AS SizeInBytes
FROM [PLS_MultiTenant_Staging].[dbo].[KEY_VALUE] order by SizeInBytes desc;

SELECT TOP 1 
      [PROPERTIES]
	  ,DATALENGTH(PROPERTIES) AS SizeInBytes
FROM [PLS_MultiTenant_Staging].[dbo].[METADATA_ATTRIBUTE] order by SizeInBytes desc;

SELECT TOP 1 
      [FEATURES]
	  ,DATALENGTH(FEATURES) AS SizeInBytes
FROM [PLS_MultiTenant_Staging].[dbo].[MODEL] order by SizeInBytes desc;

SELECT TOP 1 
      [CONFIG_DATA]
	  ,DATALENGTH(CONFIG_DATA) AS SizeInBytes
FROM [PLS_MultiTenant_Staging].[dbo].[MODELQUALITY_MODELCONFIG] order by SizeInBytes desc;

SELECT TOP 1 
      [FLAGGED_COLUMNS]
	  ,DATALENGTH(FLAGGED_COLUMNS) AS SizeInBytes
FROM [PLS_MultiTenant_Staging].[dbo].[MODELREVIEW_COLUMNRESULT] order by SizeInBytes desc;

SELECT TOP 1 
      [FLAGGED_COLUMNS]
	  ,DATALENGTH(FLAGGED_COLUMNS) AS SizeInBytes
FROM [PLS_MultiTenant_Staging].[dbo].[MODELREVIEW_DATARULE] order by SizeInBytes desc;

SELECT TOP 1 
      [PROPERTIES]
	  ,DATALENGTH(PROPERTIES) AS SizeInBytes
FROM [PLS_MultiTenant_Staging].[dbo].[MODELREVIEW_DATARULE] order by SizeInBytes desc;

SELECT TOP 1 
      [FLAGGED_ROW_TO_COLUMNS]
	  ,DATALENGTH(FLAGGED_ROW_TO_COLUMNS) AS SizeInBytes
FROM [PLS_MultiTenant_Staging].[dbo].[MODELREVIEW_ROWRESULT] order by SizeInBytes desc;

SELECT TOP 1 
      [VALUES]
	  ,DATALENGTH([VALUES]) AS SizeInBytes
FROM [PLS_MultiTenant_Staging].[dbo].[PREDICTOR_ELEMENT] order by SizeInBytes desc;

SELECT TOP 1 
      [MESSAGE]
	  ,DATALENGTH(MESSAGE) AS SizeInBytes
FROM [PLS_MultiTenant_Staging].[dbo].[TENANT_DEPLOYMENT] order by SizeInBytes desc;

SELECT TOP 1 
      [ERROR_DETAILS]
	  ,DATALENGTH(ERROR_DETAILS) AS SizeInBytes
FROM [PLS_MultiTenant_Staging].[dbo].[WORKFLOW_JOB] order by SizeInBytes desc;

SELECT TOP 1 
      [SERIALIZED_CONTEXT]
	  ,DATALENGTH(SERIALIZED_CONTEXT) AS SizeInBytes
FROM [PLS_MultiTenant_Staging].[dbo].[WORKFLOW_JOB_EXECUTION_CONTEXT] order by SizeInBytes desc;

SELECT TOP 1 
      [STRING_VAL]
	  ,DATALENGTH(STRING_VAL) AS SizeInBytes
FROM [PLS_MultiTenant_Staging].[dbo].[WORKFLOW_JOB_EXECUTION_PARAMS] order by SizeInBytes desc;

SELECT TOP 1 
      [SERIALIZED_CONTEXT]
	  ,DATALENGTH(SERIALIZED_CONTEXT) AS SizeInBytes
FROM [PLS_MultiTenant_Staging].[dbo].[WORKFLOW_STEP_EXECUTION_CONTEXT] order by SizeInBytes desc;





