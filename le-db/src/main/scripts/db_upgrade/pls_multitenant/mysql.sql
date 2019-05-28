USE `PLS_MultiTenant`;

CREATE PROCEDURE `UpdatePLSTables`()
  BEGIN	  
	  alter table `PLS_MultiTenant`.`PLAY_LAUNCH` add column `ACCOUNTS_DUPLICATED` BIGINT;
	  alter table `PLS_MultiTenant`.`PLAY_LAUNCH` add column `CONTACTS_DUPLICATED` BIGINT;
	  
	  alter table `PLS_MultiTenant`.`DATA_INTEG_STATUS_MONITORING` add column `ERROR_MESSAGE` VARCHAR(1024);
	  ALTER TABLE `PLS_MultiTenant`.`TENANT`
      CHANGE COLUMN `NOTIFICATION_STATE` `NOTIFICATION_STATE` BIT(3) NULL DEFAULT b'1' COMMENT '0. Stop all notifications 1. Send only error 3. Send error & warning notifications 7. Send all notifications  ' ;
  END;
//
DELIMITER;

CALL `UpdatePLSTables`();