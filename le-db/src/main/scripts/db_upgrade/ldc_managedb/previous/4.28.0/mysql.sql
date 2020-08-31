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
        CREATE TABLE `ContactMasterTpsColumn` (
            `PID`               BIGINT NOT NULL AUTO_INCREMENT,
            `ColumnName`        VARCHAR(100) NOT NULL,
            `JavaClass`         VARCHAR(50) NOT NULL,
            `IsDerived`         BOOLEAN NOT NULL,
            `MatchDestination`  VARCHAR(100),
            PRIMARY KEY (`PID`)
        ) ENGINE = InnoDB;

        CREATE INDEX IX_ColumnName ON `ContactMasterTpsColumn` (`ColumnName`);

        CREATE INDEX IX_MatchDestination ON `ContactMasterTpsColumn` (`MatchDestination`);

  END //
-- ##############################################################

-- DO NOT touch this part
DELIMITER ;
CALL `UpdateSchema`();
