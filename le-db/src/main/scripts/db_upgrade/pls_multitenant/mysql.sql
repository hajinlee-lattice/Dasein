USE `PLS_MultiTenant`;

/* PLS-11711 update the JSON values in PLS_MultiTenant.WORKFLOW_JOB */

UPDATE PLS_MultiTenant.WORKFLOW_JOB
SET `INPUT_CONTEXT` = JSON_REPLACE(
		`INPUT_CONTEXT`,
        '$.EXPORT_OBJECT_TYPE',
        'Enriched Accounts'
)
WHERE TYPE = 'segmentExportWorkflow'
AND JSON_EXTRACT (`INPUT_CONTEXT`, '$.EXPORT_OBJECT_TYPE') = 'Accounts';

UPDATE PLS_MultiTenant.WORKFLOW_JOB
SET `INPUT_CONTEXT` = JSON_REPLACE(
		`INPUT_CONTEXT`,
        '$.EXPORT_OBJECT_TYPE',
        'Enriched Contacts (No Account Attributes)'
)
WHERE TYPE = 'segmentExportWorkflow'
AND JSON_EXTRACT (`INPUT_CONTEXT`, '$.EXPORT_OBJECT_TYPE') = 'Contacts';

UPDATE PLS_MultiTenant.WORKFLOW_JOB
SET `INPUT_CONTEXT` = JSON_REPLACE(
		`INPUT_CONTEXT`,
        '$.EXPORT_OBJECT_TYPE',
        'Enriched Contacts with Account Attributes'
)
WHERE TYPE = 'segmentExportWorkflow'
AND JSON_EXTRACT (`INPUT_CONTEXT`, '$.EXPORT_OBJECT_TYPE') = 'Accounts and Contacts';

/* End PLS-11711 JSON update */


CREATE PROCEDURE `Update_CDL_BUSINESS_CALENDAR`()
  BEGIN
  END;
//
DELIMITER

CREATE PROCEDURE `UpdatePLSTables`()
  BEGIN
  END;
//
DELIMITER;

CREATE PROCEDURE `CreateDataCollectionArtifactTable`()
  BEGIN
  END;
//
DELIMITER ;

CREATE PROCEDURE `UpdateWorkflowJobTable`()
  BEGIN
  END;
//
DELIMITER;


CREATE PROCEDURE `CreateDropBoxTable`()
  BEGIN
  END;
//
DELIMITER;

CREATE PROCEDURE `CreateDataIntegrationMonitoringTable`()
  BEGIN
  END;
//
DELIMITER;

CREATE PROCEDURE `CreateDataIntegrationMessageTable`()
  BEGIN
  END;
//
DELIMITER;

CALL `Update_CDL_BUSINESS_CALENDAR`();
CALL `CreateDataIntegrationMonitoringTable`();
CALL `CreateDataIntegrationMessageTable`();
