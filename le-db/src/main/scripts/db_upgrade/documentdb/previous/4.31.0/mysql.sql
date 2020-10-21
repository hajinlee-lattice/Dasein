/*
* script name - mysql.sql
* purpose - Base sql file to prepare DB upgrade script.
* It contains DDL/DML sql queries that can be applied at 'release regression' & 'release window' cycle.
* Ensure to maintain backward compatibility.
*/

USE `DocumentDB`;

DROP PROCEDURE IF EXISTS `UpdateSchema`;
DELIMITER //

CREATE TABLE IF NOT EXISTS `DanteConfiguration` (
    UUID varchar(36) primary key,
    TenantId varchar(255),
    CreatedDate datetime,
    LastModifiedDate datetime,
    Document json,
    index IX_ID (TenantId),
    constraint UX_ID unique(TenantId)
)


-- ##############################################################
CREATE PROCEDURE `UpdateSchema`()
  BEGIN
      -- User input section (DDL/DML). This is just a template, developer can modify based on need.

  END //
-- ##############################################################

-- DO NOT touch this part
DELIMITER ;
CALL `UpdateSchema`();
