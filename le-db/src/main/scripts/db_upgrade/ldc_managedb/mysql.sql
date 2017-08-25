USE `LDC_ManageDB`;

SET SQL_SAFE_UPDATES = 0;

DROP PROCEDURE IF EXISTS `UpdateSchema`;

DELIMITER //
CREATE PROCEDURE `UpdateSchema`()
  BEGIN

create table `CUSTOMER_REPORT` (`PID` bigint not null auto_increment unique, `COMMENT` longtext, `CREATED_TIME` datetime, `ID` varchar(255) not null unique, `JIRA_TICKET` varchar(255), `REPORTED_BY_TENANT` varchar(255), `REPORTED_BY_USER` varchar(255), `REPRODUCEDETAIL` longtext, `SUGGESTED_VALUE` varchar(255), `TYPE` varchar(255), primary key (`PID`)) ENGINE=InnoDB;
create index Report_ID_IDX on `CUSTOMER_REPORT` (`ID`);

  END //
DELIMITER ;

CALL `UpdateSchema`();



