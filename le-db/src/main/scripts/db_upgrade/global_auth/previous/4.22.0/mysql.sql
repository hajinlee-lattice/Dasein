USE `GlobalAuthentication`;

DROP PROCEDURE IF EXISTS `UpdateGlobalTicket`;

DELIMITER //
CREATE PROCEDURE `UpdateGlobalTicket`()
  BEGIN
      ALTER TABLE `GlobalAuthentication`.`GlobalTicket` ADD COLUMN `External_Session` JSON DEFAULT NULL;
      ALTER TABLE `GlobalAuthentication`.`GlobalTicket` ADD COLUMN `Issuer` VARCHAR(255) GENERATED ALWAYS AS (`External_Session` ->> '$.Issuer');
      CREATE INDEX IX_User_Issuer ON `GlobalAuthentication`.`GlobalTicket` (`User_ID`, `Issuer`);
  END;
//
DELIMITER ;

CALL `UpdateGlobalTicket`();
