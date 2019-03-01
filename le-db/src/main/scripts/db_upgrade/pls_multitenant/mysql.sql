USE `PLS_MultiTenant`;

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
