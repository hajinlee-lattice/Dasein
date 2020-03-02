USE `GlobalAuthentication`;

DROP PROCEDURE IF EXISTS `AddGlobalTeamTable`;

DELIMITER //
CREATE PROCEDURE `AddGlobalTeamTable`()
  BEGIN
    DROP TABLE if EXISTS `GlobalTeam`;
      CREATE TABLE `GlobalTeam` (
	    `PID` bigint(20) NOT NULL AUTO_INCREMENT,
	    `Creation_Date` datetime NOT NULL,
	    `Last_Modification_Date` datetime NOT NULL,
	    `Created_By` INT(11) NOT NULL,
	    `Last_Modified_By` INT(11) NOT NULL,
	    PRIMARY KEY (`PID`)
	) ENGINE=InnoDB;

    DROP TABLE if EXISTS `GlobalTeamUser`;
    CREATE TABLE GlobalTeamUser (
      `User_ID` bigint(20) NOT NULL,
      `Team_ID` bigint(20) NOT NULL
    ) ENGINE=InnoDB;
    CREATE INDEX FK_GlobalTeamUser_UserID_GlobalUserTenantRight ON `GlobalTeamUser` (`User_ID`);
    CREATE INDEX FK_GlobalTeamUser_TeamID_GlobalTeam ON `GlobalTeamUser` (`Team_ID`);
    ALTER TABLE `GlobalTeamUser` ADD CONSTRAINT `FK_GlobalTeamUser_UserID_GlobalUserTenantRight`
      FOREIGN KEY (`User_ID`) REFERENCES `GlobalUserTenantRight` (`GlobalUserTenantRight_ID`) ON DELETE CASCADE;
    ALTER TABLE `GlobalTeamUser` ADD CONSTRAINT `FK_GlobalTeamUser_TeamID_GlobalTeam`
      FOREIGN KEY (`Team_ID`) REFERENCES `GlobalTeam` (`PID`) ON DELETE CASCADE;
  END;
//
DELIMITER ;

CALL `AddGlobalTeamTable`();