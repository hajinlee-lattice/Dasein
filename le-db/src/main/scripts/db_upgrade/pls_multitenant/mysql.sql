/*
* script name - mysql.sql
* purpose - Base sql file to prepare DB upgrade script.
* It contains DDL/DML sql queries that can be applied at 'release regression' & 'release window' cycle.
* Ensure to maintain backward compatibility.
*/

USE `PLS_MultiTenant`;

DROP PROCEDURE IF EXISTS `UpdateSchema`;
DELIMITER //

-- ##############################################################
CREATE PROCEDURE `UpdateSchema`()
  BEGIN
      -- User input section (DDL/DML). This is just a template, developer can modify based on need.
      UPDATE `PLS_MultiTenant`.`METADATA_SEGMENT`
        SET TEAM_ID='Global_Team' WHERE TEAM_ID is NULL;
      UPDATE `PLS_MultiTenant`.`RATING_ENGINE`
        SET TEAM_ID='Global_Team' WHERE TEAM_ID is NULL;


      alter table `PLS_MultiTenant`.`DCP_DATA_REPORT`
        drop FOREIGN KEY `FK_DCPDATAREPORT_FKDUNSCOUNT_METADATATABLE`;
      alter table `PLS_MultiTenant`.`DCP_DATA_REPORT`
	add constraint `FK_DCPDATAREPORT_FKDUNSCOUNT_METADATATABLE`
	foreign key (`FK_DUNS_COUNT`) references `PLS_MultiTenant`.`METADATA_TABLE` (`PID`) on delete cascade;


      CREATE TABLE `DCP_ENRICHMENT_LAYOUT`
      (
          `PID`          bigint(20)                              NOT NULL AUTO_INCREMENT,
          `LAYOUT_ID`    varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
          `DOMAIN`       varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
          `RECORD_TYPE`  varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
          `CREATED`      datetime                                NOT NULL,
          `CREATED_BY`   varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
          `DELETED`      bit(1)                                  NOT NULL,
          `TEAM_ID`      varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
          `UPDATED`      datetime                                NOT NULL,
          `FK_TENANT_ID` bigint(20)                              NOT NULL,
          PRIMARY KEY (`PID`),
          UNIQUE KEY `UX_LAYOUT_ID` (`FK_TENANT_ID`,`LAYOUT_ID`),
          KEY `IX_LAYOUT_ID` (`LAYOUT_ID`),
          CONSTRAINT `FK_DCPENRICHMENT_FKTENANTID_TENANT` FOREIGN KEY (`FK_TENANT_ID`) REFERENCES `TENANT` (`TENANT_PID`) ON DELETE CASCADE
      ) ENGINE = InnoDB
        DEFAULT CHARSET = utf8mb4
        COLLATE = utf8mb4_unicode_ci;

  END //
-- ##############################################################

-- DO NOT touch this part
DELIMITER ;
CALL `UpdateSchema`();
