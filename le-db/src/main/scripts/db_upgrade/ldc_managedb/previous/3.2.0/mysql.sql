USE `LDC_ManageDB`;

SET SQL_SAFE_UPDATES = 0;

DROP PROCEDURE IF EXISTS `UpdateSchema`;

DROP PROCEDURE IF EXISTS `UpdateDataCloudVersionTable`;

DROP PROCEDURE IF EXISTS `UpdateAccountMasterFact`;

DELIMITER //
CREATE PROCEDURE `UpdateDataCloudVersionTable`()
  BEGIN

    # add column if not exists
    IF NOT EXISTS(SELECT *
                  FROM information_schema.COLUMNS
                  WHERE
                    TABLE_SCHEMA = 'LDC_ManageDB'
                    AND TABLE_NAME = 'DataCloudVersion'
                    AND COLUMN_NAME = 'DynamoTableSignature_Lookup')
    THEN
      ALTER TABLE `DataCloudVersion`
        ADD `DynamoTableSignature_Lookup` VARCHAR(100);

      UPDATE `DataCloudVersion`
      SET `DynamoTableSignature_Lookup` = `DynamoTableSignature`;

    END IF;

  END //
DELIMITER ;

DELIMITER //
CREATE PROCEDURE `UpdateAccountMasterFact`()
  BEGIN

    # add column if not exists
    IF NOT EXISTS(SELECT *
                  FROM information_schema.COLUMNS
                  WHERE
                    TABLE_SCHEMA = 'LDC_ManageDB'
                    AND TABLE_NAME = 'AccountMasterFact'
                    AND COLUMN_NAME = 'GroupTotal')
    THEN
      ALTER TABLE `AccountMasterFact`
        ADD `GroupTotal` BIGINT;
      ALTER TABLE `AccountMasterFact`
        ADD `AttrCount1` LONGTEXT;
      ALTER TABLE `AccountMasterFact`
        ADD `AttrCount2` LONGTEXT;
      ALTER TABLE `AccountMasterFact`
        ADD `AttrCount3` LONGTEXT;
      ALTER TABLE `AccountMasterFact`
        ADD `AttrCount4` LONGTEXT;
    END IF;

  END //
DELIMITER ;

DELIMITER //
CREATE PROCEDURE `UpdateSchema`()
  BEGIN

    CALL `UpdateDataCloudVersionTable`();
    CALL `UpdateAccountMasterFact`();

  END //
DELIMITER ;

CALL `UpdateSchema`();



