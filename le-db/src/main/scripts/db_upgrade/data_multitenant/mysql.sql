/*
* script name - mysql.sql
* purpose - Base sql file to prepare DB upgrade script.
* It contains DDL/DML sql queries that can be applied at 'release regression' & 'release window' cycle.
* Ensure to maintain backward compatibility.
*/

USE `Data_MultiTenant`;

DROP PROCEDURE IF EXISTS `UpdateSchema`;
DELIMITER //

-- ##############################################################
CREATE PROCEDURE `UpdateSchema`()
  BEGIN
      -- User input section (DDL/DML). This is just a template, developer can modify based on need.
      create table `ActivityAlert`
      (
          `PID`                bigint       not null auto_increment,
          `ALERT_DATA`         JSON,
          `ALERT_NAME`         varchar(255) not null,
          `CATEGORY`           varchar(255) not null,
          `CREATION_TIMESTAMP` datetime     not null,
          `ENTITY_ID`          varchar(255) not null,
          `ENTITY_TYPE`        varchar(255) not null,
          `TENANT_ID`          bigint       not null,
          `VERSION`            varchar(255) not null,
          primary key (`PID`, `ENTITY_ID`, `TENANT_ID`)
      )
          ENGINE = InnoDB
          PARTITION BY KEY (`ENTITY_ID`, `TENANT_ID`)
              PARTITIONS 2000;

      create index REC_CREATION_TIMESTAMP on `ActivityAlert` (`CREATION_TIMESTAMP`);
      create index REC_VERSION on `ActivityAlert` (`VERSION`);
      create index REC_CATEGORY on `ActivityAlert` (`CATEGORY`);
      alter table `ActivityAlert`
          add constraint `UK3gmot6qm9hdifa50vhnchx2kq`
              unique (`ENTITY_ID`, `ENTITY_TYPE`, `TENANT_ID`, `VERSION`, `CREATION_TIMESTAMP`, `ALERT_NAME`);

  END //
-- ##############################################################

-- DO NOT touch this part
DELIMITER ;
CALL `UpdateSchema`();
