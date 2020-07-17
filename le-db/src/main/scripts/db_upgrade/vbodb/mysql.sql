/*
* script name - mysql.sql
* purpose - Base sql file to prepare DB upgrade script. This file DDL/DML can be applied at 'release regression' & 'release window' cycle
* Rule - Contains DDL/DML sql queries.  Should maintain backward compatibility.
*/

USE `VboDB`;

DROP PROCEDURE IF EXISTS `UpdateSchema`;
DELIMITER //

-- ##############################################################
CREATE PROCEDURE `UpdateSchema`()
  BEGIN
      DROP TABLE IF EXISTS `VBO_REQUEST_LOG`;

      CREATE TABLE `VBO_REQUEST_LOG`
        (
           `PID`          BIGINT NOT NULL auto_increment,
           `TENANT_ID`    VARCHAR(255),
           `TRACE_ID`     VARCHAR(255),
           `VBO_REQUEST`  JSON,
           `VBO_RESPONSE` JSON,
           PRIMARY KEY (`PID`)
        )
      engine=InnoDB;

      CREATE INDEX IX_TENANT_ID ON `VBO_REQUEST_LOG` (`TENANT_ID`);

      CREATE INDEX IX_TRACE_ID ON `VBO_REQUEST_LOG` (`TRACE_ID`);

      ALTER TABLE `VBO_REQUEST_LOG`
        ADD CONSTRAINT UX_TRACE_ID UNIQUE (`TRACE_ID`);

  END //
-- ##############################################################

-- DO NOT touch this part
DELIMITER ;
CALL `UpdateSchema`();
