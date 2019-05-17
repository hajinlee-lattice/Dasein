USE `PLS_MultiTenant`;

CREATE PROCEDURE `UpdatePLSTables`()
  BEGIN	  
	  alter table `PLS_MultiTenant`.`PLAY_LAUNCH` add column `ACCOUNTS_DUPLICATED` BIGINT;
	  alter table `PLS_MultiTenant`.`PLAY_LAUNCH` add column `CONTACTS_DUPLICATED` BIGINT;
  END;
//
DELIMITER;

CALL `UpdatePLSTables`();