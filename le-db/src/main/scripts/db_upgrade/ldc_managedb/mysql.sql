/*
* script name - mysql.sql
* purpose - Base sql file to prepare DB upgrade script.
* It contains DDL/DML sql queries that can be applied at 'release regression' & 'release window' cycle.
* Ensure to maintain backward compatibility.
*/

USE `LDC_ManageDB`;

DROP PROCEDURE IF EXISTS `UpdateSchema`;
DELIMITER //

-- ##############################################################
CREATE PROCEDURE `UpdateSchema`()
  BEGIN
      -- User input section (DDL/DML). This is just a template, developer can modify based on need.
      create table `VboUsageReport` (`PID` bigint not null auto_increment, `CreatedAt` datetime not null, `NumRecords` bigint, `QuietPeriodEnd` datetime, `ReportId` varchar(100) not null, `S3Bucket` varchar(50) not null, `S3Path` varchar(500) not null, `SubmittedAt` datetime, `Type` varchar(255) not null, primary key (`PID`)) engine=InnoDB;
      create index IX_QUIET_PERIOD on `VboUsageReport` (`QuietPeriodEnd`);
      create index IX_SUBMITTED_AT on `VboUsageReport` (`SubmittedAt`);
      alter table `VboUsageReport` add constraint UX_REPORT_ID unique (`ReportId`);

  END //
-- ##############################################################

-- DO NOT touch this part
DELIMITER ;
CALL `UpdateSchema`();
