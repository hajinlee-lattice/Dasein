USE `GlobalAuthentication`;

DROP PROCEDURE IF EXISTS `UpdateTable`;
DELIMITER //
  BEGIN
   CREATE PROCEDURE `UpdateTable`()
      alter table `SECURITY_IDENTITY_PROVIDER` add constraint UX_IDENTITY_ID unique (`TENANT_ID`, `ENTITY_ID`);
    ALTER TABLE `GlobalTeam` ADD CONSTRAINT `FK_GlobalTeam_TenantID_GlobalTenant` FOREIGN KEY (`Tenant_ID`) REFERENCES
        `GlobalTenant` (`GlobalTenant_ID`) ON DELETE CASCADE;
    ALTER TABLE `GlobalUserTenantRight` ADD CONSTRAINT `UKbgb06swixji98da137u00ra4c` unique (`Tenant_ID`, `User_ID`);
  END;
//
DELIMITER ;

CALL `UpdateTable`();
