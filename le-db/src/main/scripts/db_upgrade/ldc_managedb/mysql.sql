USE `LDC_ManageDB`;

SET SQL_SAFE_UPDATES = 0;

DROP PROCEDURE IF EXISTS `UpdateDataCloudVersionTable`;

DROP PROCEDURE IF EXISTS `UpdateSchema`;

DELIMITER //
CREATE PROCEDURE `UpdateDataCloudVersionTable`()
  BEGIN

    # add column if not exists
    IF NOT EXISTS(SELECT *
                  FROM information_schema.COLUMNS
                  WHERE
                    TABLE_SCHEMA = 'LDC_ManageDB'
                    AND TABLE_NAME = 'DataCloudVersion'
                    AND COLUMN_NAME = 'DynamoTableSignature_DunsGuideBook')
    THEN
      ALTER TABLE `DataCloudVersion`
        ADD COLUMN `DynamoTableSignature_DunsGuideBook` VARCHAR(100)
        NULL DEFAULT NULL AFTER `DynamoTableSignature_Lookup`;

    END IF;

  END //
DELIMITER ;

DELIMITER //
CREATE PROCEDURE `UpdateSchema`()
  BEGIN
    CALL `UpdateDataCloudVersionTable`();

    START TRANSACTION;

create table `CustomerReport` (`PID` bigint not null auto_increment unique, `COMMENT` longtext, `CREATED_TIME` datetime, `ID` varchar(255) not null unique, `JIRA_TICKET` varchar(255), `REPORTED_BY_TENANT` varchar(255), `REPORTED_BY_USER` varchar(255), `REPRODUCEDETAIL` longtext, `SUGGESTED_VALUE` varchar(255), `TYPE` varchar(255), primary key (`PID`)) ENGINE=InnoDB;
create index Report_ID_IDX on `CustomerReport` (`ID`);
ALTER TABLE `CustomerReport` ADD `INCORRECT_ATTRIBUTE` varchar(255);
    COMMIT;
  END //
DELIMITER ;

CALL `UpdateSchema`();



