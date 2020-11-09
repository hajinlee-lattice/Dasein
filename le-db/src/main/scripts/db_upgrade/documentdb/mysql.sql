/*
* script name - mysql.sql
* purpose - Base sql file to prepare DB upgrade script.
* It contains DDL/DML sql queries that can be applied at 'release regression' & 'release window' cycle.
* Ensure to maintain backward compatibility.
*/

USE `DocumentDB`;

DROP PROCEDURE IF EXISTS `UpdateSchema`;
DELIMITER //

-- ##############################################################
CREATE PROCEDURE `UpdateSchema`()
  BEGIN
      -- User input section (DDL/DML). This is just a template, developer can modify based on need.
      CREATE TABLE `DataTemplate` (
        `UUID` varchar(36) NOT NULL,
        `CreatedDate` datetime,
        `Document` json,
        `LastModifiedDate` datetime,
        `TenantId` varchar(255) NOT NULL,
        PRIMARY KEY (`UUID`)
      ) engine = InnoDB;

      CREATE INDEX IX_TENANTID ON `DataTemplate` (`TenantId`,`UUID`);

      ALTER TABLE `DataUnit`
        ADD COLUMN `DataTemplateId` VARCHAR(200) GENERATED ALWAYS AS (json_unquote(json_extract(`Document`,'$.DataTemplateId'))) VIRTUAL;

      CREATE INDEX IX_DATATEMPLATEID ON `DataUnit` (`TenantId`,`DataTemplateId`);


  END //
-- ##############################################################

-- DO NOT touch this part
DELIMITER ;
CALL `UpdateSchema`();
