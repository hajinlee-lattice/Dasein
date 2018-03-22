CREATE PROCEDURE `UpdateCDLTables`()
    BEGIN

    ALTER TABLE `PLS_MultiTenant`.`RATING_ENGINE` ADD COLUMN `ADVANCED_RATING_CONFIG` longtext;
    
    ALTER TABLE `PLS_MultiTenant`.`RATING_ENGINE` ADD COLUMN `DELETED` bit(1) DEFAULT 0;
        
    ALTER TABLE `PLS_MultiTenant`.`AI_MODEL` ADD COLUMN `ADVANCED_MODELING_CONFIG` longtext;
        
    ALTER TABLE `PLS_MultiTenant`.`BUCKET_METADATA` ADD COLUMN `RATING_ENGINE_ID` bigint;
        
    ALTER TABLE `PLS_MultiTenant`.`BUCKET_METADATA` add constraint `FK_BUCKETMETADATA_RATINGENGINEID_RATINGENGINE` foreign key (`RATING_ENGINE_ID`) references `RATING_ENGINE` (`PID`) on delete cascade;

    alter table DATAFEED DROP COLUMN `ACTIVE_PROFILE`;
    drop table DATAFEED_PROFILE;
    alter table DATAFEED_EXECUTION ADD COLUMN `JOB_TYPE` varchar(255) not null DEFAULT 'PA';
    UPDATE DATAFEED_EXECUTION SET STATUS = 'Completed' where STATUS = 'ProcessAnalyzed';
    
    create table `BUCKETED_SCORE_SUMMARY` (`PID` bigint not null auto_increment, `BAR_LIFTS` JSON not null, `BUCKETED_SCORES` JSON not null, `OVERAL_LIFT` double precision not null, `TOTAL_NUM_CONVERTED` integer not null, `TOTAL_NUM_LEADS` integer not null, `FK_MODELSUMMARY_ID` bigint not null, primary key (`PID`)) engine=InnoDB;
    alter table `BUCKETED_SCORE_SUMMARY` add constraint `FK_BUCKETEDSCORESUMMARY_FKMODELSUMMARYID_MODELSUMMARY` foreign key (`FK_MODELSUMMARY_ID`) references `MODEL_SUMMARY` (`PID`) on delete cascade;

    END;
//
DELIMITER ;


CREATE PROCEDURE `AddCDLActivityMetrics`()
    BEGIN
	CREATE TABLE IF NOT EXISTS `CDL_ACTIVITY_METRICS` (
	  `PID` bigint(20) NOT NULL AUTO_INCREMENT,
	  `CREATED` datetime NOT NULL,
	  `DEPRECATED` datetime DEFAULT NULL,
	  `IS_EOL` bit(1) DEFAULT NULL,
	  `METRICS` varchar(100) NOT NULL,
	  `PERIODS` varchar(1000) NOT NULL,
	  `TYPE` varchar(100) NOT NULL,
	  `UPDATED` datetime NOT NULL,
	  `FK_TENANT_ID` bigint(20) NOT NULL,
	  PRIMARY KEY (`PID`),
	  KEY `FK_CDLACTIVITYMETRICS_FKTENANTID_TENANT` (`FK_TENANT_ID`),
	  CONSTRAINT `FK_CDLACTIVITYMETRICS_FKTENANTID_TENANT` FOREIGN KEY (`FK_TENANT_ID`) REFERENCES `TENANT` (`TENANT_PID`) ON DELETE CASCADE
	) ENGINE=InnoDB;
    END
//
DELIMITER ;


DELIMITER //
CREATE PROCEDURE `UpdateSchema`()
    BEGIN
        START TRANSACTION;
        CALL `UpdateCDLTables`();
	CALL `AddCDLActivityMetrics`();
        COMMIT;
    END;
//
DELIMITER ;

CALL `UpdateSchema`();
