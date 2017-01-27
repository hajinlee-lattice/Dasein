USE `LDC_ManageDB`;

DROP PROCEDURE IF EXISTS `UpdateSchema`;

DROP PROCEDURE IF EXISTS `UpdateDataCloudVersionTable`;

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
    END IF;

  END //
DELIMITER ;

DELIMITER //
CREATE PROCEDURE `UpdateSchema`()
  BEGIN

    CALL `UpdateDataCloudVersionTable`();

  END //
DELIMITER ;

CALL `UpdateSchema`();



