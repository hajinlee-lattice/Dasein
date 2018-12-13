USE `GlobalAuthentication`;

DROP PROCEDURE IF EXISTS `UpdateSchema`;
DROP PROCEDURE IF EXISTS `AddGlobalUserTenantConfig`;

DELIMITER //
CREATE PROCEDURE `AddGlobalUserTenantConfig`()
  BEGIN
    create table `GlobalUserTenantConfig` (`GlobalUserTenantConfig_ID` bigint not null auto_increment, `Created_By` integer not null, 
        `Creation_Date` datetime not null, `Last_Modification_Date` datetime not null, `Last_Modified_By` integer not null, `Config_Property` varchar(255), 
        `Property_Value` varchar(255), `Tenant_ID` bigint not null, `User_ID` bigint not null, primary key (`GlobalUserTenantConfig_ID`));
        
    alter table `GlobalUserTenantConfig` add constraint FK_GAUTCONFIG_GATENANT 
        foreign key (`Tenant_ID`) references `GlobalTenant` (`GlobalTenant_ID`) on delete cascade;
    alter table `GlobalUserTenantConfig` add constraint FK_GAUTCONFIG_GAUSER 
        foreign key (`User_ID`) references `GlobalUser` (`GlobalUser_ID`) on delete cascade;
  END;
//
DELIMITER ;

DELIMITER //
CREATE PROCEDURE `UpdateGlobalTenantConfig`()
  BEGIN
      ALTER TABLE `GlobalAuthentication`.`GlobalTenant` ADD COLUMN `Created_By_User` varchar(255);
      ALTER TABLE `GlobalAuthentication`.`GlobalUser` ADD COLUMN `Created_By_User` varchar(255);
      ALTER TABLE `GlobalAuthentication`.`GlobalUserTenantRight` ADD COLUMN `Created_By_User` varchar(255);
  END;
//
DELIMITER ;

DELIMITER //
CREATE PROCEDURE `UpdateSchema`()
  BEGIN
    START TRANSACTION;
    CALL `AddGlobalUserTenantConfig`();
    COMMIT;
  END;
//
DELIMITER ;

CALL `UpdateSchema`();
CALL `UpdateGlobalTenantConfig`();
