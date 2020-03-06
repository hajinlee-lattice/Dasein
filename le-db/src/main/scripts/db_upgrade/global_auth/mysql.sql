USE `GlobalAuthentication`;
DROP PROCEDURE IF EXISTS `AddGlobalTeamTable`;
DELIMITER //
CREATE PROCEDURE `AddGlobalTeamTable`()
  BEGIN
    DROP TABLE if EXISTS `GlobalTeam`;
      CREATE TABLE `GlobalTeam` (
	    `GlobalTeam_ID` bigint(20) NOT NULL AUTO_INCREMENT,
	    `Creation_Date` datetime NOT NULL,
	    `Last_Modification_Date` datetime NOT NULL,
	    `Created_By` INT(11) NOT NULL,
	    `Last_Modified_By` INT(11) NOT NULL,
	    `NAME`  VARCHAR(255) NOT NULL,
	    `Tenant_ID` bigint(20) NOT NULL,
	    `Created_By_User` VARCHAR(255),
	    PRIMARY KEY (`GlobalTeam_ID`)
	) ENGINE=InnoDB;

	ALTER TABLE `GlobalTeam` ADD CONSTRAINT `FK_GlobalTeam_TenantID_GlobalTenant` FOREIGN KEY (`Tenant_ID`)
	  REFERENCES `GlobalTenant` (`GlobalTenant_ID`) ON DELETE CASCADE;

    ALTER TABLE `GlobalUserTenantRight` ADD CONSTRAINT `UKeu0uqycmovglkyn6yudo4eih1` unique (`Operation_Name`, `Tenant_ID`, `User_ID`);

    DROP TABLE if EXISTS `GlobalTeamTenantMember`;
    CREATE TABLE GlobalTeamTenantMember (
      `Team_ID` bigint(20) NOT NULL,
      `TenantMember_ID` bigint(20) NOT NULL
    ) ENGINE=InnoDB;
    CREATE INDEX FK_GlobalTeamTenantMember_TeamID_GlobalTeam ON `GlobalTeamTenantMember` (`Team_ID`);
    CREATE INDEX FK_GlobalTeamTenantMember_TenantMemberID_GlobalUserTenantRight ON `GlobalTeamTenantMember` (`TenantMember_ID`);
    ALTER TABLE `GlobalTeamTenantMember` ADD CONSTRAINT `FK_GlobalTeamTenantMember_TeamID_GlobalTeam`
      FOREIGN KEY (`Team_ID`) REFERENCES `GlobalTeam` (`GlobalTeam_ID`) ON DELETE CASCADE;
    ALTER TABLE `GlobalTeamTenantMember` ADD CONSTRAINT `FK_GlobalTeamTenantMember_TenantMemberID_GlobalUserTenantRight`
      FOREIGN KEY (`TenantMember_ID`) REFERENCES `GlobalUserTenantRight` (`GlobalUserTenantRight_ID`) ON DELETE CASCADE;
  END;
//
DELIMITER ;
CALL `AddGlobalTeamTable`();
