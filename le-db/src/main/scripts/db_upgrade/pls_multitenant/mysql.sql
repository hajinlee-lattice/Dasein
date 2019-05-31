USE `PLS_MultiTenant`;

CREATE PROCEDURE `UpdatePLSTables`()
  BEGIN	  
	  alter table `PLS_MultiTenant`.`PLAY_LAUNCH` add column `ACCOUNTS_DUPLICATED` BIGINT;
	  alter table `PLS_MultiTenant`.`PLAY_LAUNCH` add column `CONTACTS_DUPLICATED` BIGINT;
	  
	  alter table `PLS_MultiTenant`.`DATA_INTEG_STATUS_MONITORING` add column `ERROR_MESSAGE` VARCHAR(1024);
	  ALTER TABLE `PLS_MultiTenant`.`TENANT`
      ADD COLUMN `NOTIFICATION_LEVEL` VARCHAR(20) NULL DEFAULT 'ERROR' AFTER `EXPIRED_TIME`;
      
      alter table `PLS_MultiTenant`.`PLAY_LAUNCH` add column `CONTACTS_SELECTED` BIGINT;
      alter table `PLS_MultiTenant`.`PLAY_LAUNCH` add column `CONTACTS_SUPPRESSED` BIGINT;

  END;
//
DELIMITER;

CALL `UpdatePLSTables`();