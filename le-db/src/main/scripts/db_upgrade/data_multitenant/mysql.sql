USE `Data_MultiTenant`;

DROP PROCEDURE IF EXISTS `UpdateSchema`;

DELIMITER //
CREATE PROCEDURE `UpdateRecommendation`()
  BEGIN
    CREATE TABLE `Recommendation` (`PID` bigint not null auto_increment unique, `ACCOUNT_ID` varchar(255) not null, `COMPANY_NAME` varchar(255), `CONTACTS` varchar(255), `DESCRIPTION` varchar(255), `LAST_UPDATED_TIMESTAMP` datetime not null, `LAUNCH_DATE` datetime not null, `LAUNCH_ID` varchar(255) not null, `LE_ACCOUNT_EXTERNAL_ID` varchar(255) not null, `LIKELIHOOD` double precision, `MONETARY_VALUE` double precision, `MONETARY_VALUE_ISO4217_ID` varchar(255), `PLAY_ID` varchar(255) not null, `PRIORITY_DISPLAY_NAME` varchar(255), `PRIORITY_ID` varchar(255), `EXTERNAL_ID` varchar(255) not null, `SFDC_ACCOUNT_ID` varchar(255), `SYNC_DESTINATION` varchar(255), `TENANT_ID` bigint not null, primary key (`PID`)) ENGINE=InnoDB;

    CREATE INDEX REC_LAUNCH_LAST_UPD_TIME on `Recommendation` (`LAST_UPDATED_TIMESTAMP`);
    CREATE INDEX REC_LAUNCH_DATE on `Recommendation` (`LAUNCH_DATE`);
    CREATE INDEX REC_LAUNCH_ID on `Recommendation` (`LAUNCH_ID`);
    CREATE INDEX REC_PLAY_ID on `Recommendation` (`PLAY_ID`);
    CREATE INDEX REC_EXTERNAL_ID on `Recommendation` (`EXTERNAL_ID`);
    CREATE INDEX REC_TENANT_ID on `Recommendation` (`TENANT_ID`);
  END;
//
DELIMITER ;

DELIMITER //
CREATE PROCEDURE `UpdateSchema`()
  BEGIN
    START TRANSACTION;
    CALL `UpdateRecommendation`();
    COMMIT;
  END;
//
DELIMITER ;

CALL `UpdateSchema`();



