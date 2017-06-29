USE `Data_MultiTenant`;

DROP PROCEDURE IF EXISTS `UpdateSchema`;

DELIMITER //
CREATE PROCEDURE `UpdateRecommendation`()
  BEGIN
    CREATE TABLE `Recommendation` (`PID` bigint not null auto_increment unique, `CREATED_TIMESTAMP` datetime not null, `DESCRIPTION` varchar(255), `LAST_UPDATED_TIMESTAMP` datetime not null, `LAUNCH_ID` varchar(255) not null, `PLAY_ID` varchar(255) not null, `EXTERNAL_ID` varchar(255) not null, `TENANT_ID` bigint not null, primary key (`PID`)) ENGINE=InnoDB;
    CREATE INDEX REC_LAUNCH_CREATED_TIME on `Recommendation` (`CREATED_TIMESTAMP`);
    CREATE INDEX REC_LAUNCH_LAST_UPD_TIME on `Recommendation` (`LAST_UPDATED_TIMESTAMP`);
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



