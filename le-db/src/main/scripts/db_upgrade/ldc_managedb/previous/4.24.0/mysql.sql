USE `LDC_ManageDB`;

SET SQL_SAFE_UPDATES = 0;

DROP PROCEDURE IF EXISTS `UpdateDataCloudVersionTable`;

DROP PROCEDURE IF EXISTS `UpdateDecisionGraphTable`;

DROP PROCEDURE IF EXISTS `UpdateAccountMasterFact`;

DROP PROCEDURE IF EXISTS `UpdateIngestionProgress`;

DROP PROCEDURE IF EXISTS `UpdateSchema`;

DROP PROCEDURE IF EXISTS `DropTables`;

DELIMITER //
CREATE PROCEDURE `UpdateDataCloudVersionTable`()
  BEGIN
  END //
DELIMITER ;

DELIMITER //
CREATE PROCEDURE `UpdateAccountMasterFact`()
  BEGIN
	  ALTER TABLE AccountMasterFact
		DROP COLUMN DataCloudVersion;
  END //
DELIMITER ;

DELIMITER //
CREATE PROCEDURE `UpdateIngestionProgress`()
  BEGIN
	  CREATE INDEX IX_VERSION
		ON IngestionProgress (Version);
	  CREATE INDEX IX_INGESTION_ID
		ON IngestionProgress (IngestionId);
  END //
DELIMITER ;

DELIMITER //
CREATE PROCEDURE `DropTables`()
  BEGIN
	  DROP TABLE Orchestration_Copy;
  END //
DELIMITER ;

DELIMITER //
CREATE PROCEDURE `UpdateDecisionGraphTable`()
  BEGIN
  END //
DELIMITER ;

DELIMITER //
CREATE PROCEDURE `UpdateMatchCommandTable`()
  BEGIN
      # add column if not exists
      IF NOT EXISTS(SELECT *
                    FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA = 'LDC_ManageDB'
                      AND TABLE_NAME = 'MatchCommand'
                      AND COLUMN_NAME = 'NewEntityCounts')
      THEN
          ALTER TABLE MatchCommand
              ADD COLUMN NewEntityCounts JSON DEFAULT NULL;
      END IF;
  END //
DELIMITER ;

DELIMITER //
CREATE PROCEDURE `UpdateSchema`()
  BEGIN
      CALL `UpdateMatchCommandTable`();
      CALL `UpdateAccountMasterFact`();
      CALL `UpdateIngestionProgress`();
  END //
DELIMITER ;

CALL `UpdateSchema`();
CALL `DropTables`();



