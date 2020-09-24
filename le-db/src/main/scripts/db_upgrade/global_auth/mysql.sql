/*
* script name - mysql.sql
* purpose - Base sql file to prepare DB upgrade script.
* It contains DDL/DML sql queries that can be applied at 'release regression' & 'release window' cycle.
* Ensure to maintain backward compatibility.
*/

USE `GlobalAuthentication`;

DROP PROCEDURE IF EXISTS `UpdateSchema`;
DELIMITER //

-- ##############################################################
CREATE PROCEDURE `UpdateSchema`()
  BEGIN
      -- User input section (DDL/DML). This is just a template, developer can modify based on need.
      DROP TABLE if EXISTS `GlobalSubscription`;
      CREATE TABLE `GlobalSubscription` (
        `GlobalSubscription_ID` bigint(20) NOT NULL AUTO_INCREMENT,
        `UserTenantRight_ID` bigint(20) NOT NULL UNIQUE,
        `Tenant_ID` VARCHAR(255) NOT NULL,
        `Creation_Date` datetime NOT NULL,
        `Last_Modification_Date` datetime NOT NULL,
        `Created_By` INT(11) NOT NULL,
        `Last_Modified_By` INT(11) NOT NULL,
        PRIMARY KEY (`GlobalSubscription_ID`)
      ) ENGINE=InnoDB;
      CREATE INDEX IX_TENANT_ID on `GlobalSubscription` (`Tenant_ID`);
      ALTER TABLE `GlobalSubscription` ADD CONSTRAINT `FK_GlobalSubscription_UserTenantRightID_GlobalUserTenantRight` FOREIGN KEY (`UserTenantRight_ID`) REFERENCES `GlobalUserTenantRight` (`GlobalUserTenantRight_ID`) ON DELETE CASCADE;
  END //
-- ##############################################################

-- DO NOT touch this part
DELIMITER ;
CALL `UpdateSchema`();
