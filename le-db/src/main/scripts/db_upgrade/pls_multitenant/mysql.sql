USE `PLS_MultiTenant`;

CREATE PROCEDURE `Update_SEGEMENT_EXPORT_OBJECT_TYPES`()
    BEGIN
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
    END;

CREATE PROCEDURE `Update_CDL_BUSINESS_CALENDAR`()
  BEGIN
  END;
//
DELIMITER

CREATE PROCEDURE `UpdatePLSTables`()
  BEGIN
	ALTER TABLE `PLS_MultiTenant`.`TENANT` ADD COLUMN `EXPIRED_TIME` bigint DEFAULT NULL;
	ALTER TABLE `PLS_MultiTenant`.`WORKFLOW_JOB`
    ADD COLUMN `ERROR_CATEGORY` VARCHAR(255) NULL DEFAULT 'UNKNOWN' AFTER `FK_TENANT_ID`;
    ALTER TABLE `PLS_MultiTenant`.`PLAY_LAUNCH` ADD COLUMN `FOLDER_NAME` VARCHAR(255);

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

CALL `Update_SEGEMENT_EXPORT_OBJECT_TYPES`();

CALL `Update_CDL_BUSINESS_CALENDAR`();
CALL `CreateDataIntegrationMonitoringTable`();
CALL `CreateDataIntegrationMessageTable`();
