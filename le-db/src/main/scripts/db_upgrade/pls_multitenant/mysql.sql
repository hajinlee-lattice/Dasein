USE `PLS_MultiTenant`;

CREATE PROCEDURE `CreatePlayTypes`()
BEGIN
  DECLARE tenantId INT;
  DECLARE playTypeCount INT;
  DECLARE finished INT DEFAULT 0;
  DECLARE tenants TEXT;
  DECLARE curs CURSOR FOR  SELECT distinct FK_TENANT_ID FROM PLS_MultiTenant.PLAY;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET finished=1;
  SET tenants = 'Created Play types for tenantIds: ';

  OPEN curs;
  WHILE NOT finished DO
    FETCH curs INTO tenantId;
	Select count(PID) INTO playTypeCount FROM PLS_MultiTenant.PLAY_TYPE WHERE FK_TENANT_ID = TenantId;
    IF (playTypeCount < 1) THEN
       INSERT INTO `PLS_MultiTenant`.`PLAY_TYPE`
		(`CREATED`, `CREATED_BY`, `DISPLAY_NAME`, `ID`, `UPDATED`, `UPDATED_BY`, `FK_TENANT_ID`, `TENANT_ID`)
		VALUES
		(NOW(), 'bnguyen@lattice-engines.com', 'List', UUID(), NOW(), 'bnguyen@lattice-engines.com', tenantId, tenantId),
		(NOW(), 'bnguyen@lattice-engines.com', 'Cross-Sell', UUID(), NOW(), 'bnguyen@lattice-engines.com', tenantId, tenantId),
		(NOW(), 'bnguyen@lattice-engines.com', 'Prospecting', UUID(), NOW(), 'bnguyen@lattice-engines.com', tenantId, tenantId),
		(NOW(), 'bnguyen@lattice-engines.com', 'Renewal', UUID(), NOW(), 'bnguyen@lattice-engines.com', tenantId, tenantId),
		(NOW(), 'bnguyen@lattice-engines.com', 'Upsell', UUID(), NOW(), 'bnguyen@lattice-engines.com', tenantId, tenantId);
		SET tenants = concat(tenants , tenantId, ', ');
    END IF;
  END WHILE;
  SELECT tenants;
  CLOSE curs;
END
//
DELIMITER;

CREATE PROCEDURE `AttachPlayTypes`()
BEGIN
  DECLARE tenantId INT;
  DECLARE playId INT;
  DECLARE finished INT DEFAULT 0;
  DECLARE ratingEngineType varchar(255);

  DECLARE curs CURSOR FOR SELECT p.PID, p.FK_TENANT_ID, re.TYPE
	FROM PLS_MultiTenant.PLAY p
		JOIN PLS_MultiTenant.RATING_ENGINE re ON p.FK_RATING_ENGINE_ID = re.PID;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET finished=1;
  OPEN curs;
  WHILE NOT finished DO
    FETCH curs INTO playId, tenantId, ratingEngineType;
	UPDATE PLS_MultiTenant.PLAY
	SET FK_PLAY_TYPE = CASE
		WHEN ratingEngineType = 'RULE_BASED'
			THEN (SELECT PID FROM PLS_MultiTenant.PLAY_TYPE WHERE FK_TENANT_ID = tenantId AND DISPLAY_NAME = 'List')
		WHEN ratingEngineType =	'CROSS_SELL'
			THEN (SELECT PID FROM PLS_MultiTenant.PLAY_TYPE WHERE FK_TENANT_ID = tenantId AND DISPLAY_NAME = 'Cross-Sell')
		WHEN ratingEngineType = 'CUSTOM_EVENT'
			THEN (SELECT PID FROM PLS_MultiTenant.PLAY_TYPE WHERE FK_TENANT_ID = tenantId AND DISPLAY_NAME = 'Prospecting')
		ELSE (SELECT PID FROM PLS_MultiTenant.PLAY_TYPE WHERE FK_TENANT_ID = tenantId AND DISPLAY_NAME = 'List')
		END
	WHERE PID = playId;
  END WHILE;
  CLOSE curs;
END
//
DELIMITER;

CREATE PROCEDURE `UpdatePLSTables`()
  BEGIN
    ALTER TABLE `PLS_MultiTenant`.`TENANT` ADD COLUMN `CONTRACT` varchar(255) DEFAULT NULL;
    update `PLS_MultiTenant`.`TENANT` set STATUS='ACTIVE' where STATUS is NULL;
    ALTER TABLE `PLS_MultiTenant`.`TENANT` MODIFY `STATUS` varchar(255) NOT NULL;

    update `PLS_MultiTenant`.`TENANT` set TENANT_TYPE = 'CUSTOMER' where TENANT_TYPE is NULL;
    ALTER TABLE `PLS_MultiTenant`.`TENANT` MODIFY `TENANT_TYPE` varchar(255) NOT NULL;

    ALTER TABLE `PLS_MultiTenant`.`PLAY` MODIFY COLUMN `DESCRIPTION` varchar(255) default NULL;

    create table `PLS_MultiTenant`.`PLAY_TYPE` (
    `PID` bigint not null auto_increment,
    `CREATED` datetime not null,
    `CREATED_BY` varchar(255) not null,
    `DESCRIPTION` varchar(8192),
    `DISPLAY_NAME` varchar(255) not null,
    `ID` varchar(255) not null,
    `TENANT_ID` bigint not null,
    `UPDATED` datetime not null,
    `UPDATED_BY` varchar(255) not null,
    `FK_TENANT_ID` bigint not null,
    primary key (`PID`))
    engine=InnoDB;

    alter table `PLS_MultiTenant`.`PLAY_TYPE` add constraint `FK_PLAYTYPE_FKTENANTID_TENANT` foreign key (`FK_TENANT_ID`) references `PLS_MultiTenant`.`TENANT` (`TENANT_PID`) on delete cascade;

    CALL `CreatePlayTypes`();

    alter table `PLS_MultiTenant`.`PLAY` add column `FK_PLAY_TYPE` bigint;
    alter table `PLS_MultiTenant`.`PLAY` add constraint `FK_PLAY_FKPLAYTYPE_PLAYTYPE` foreign key (`FK_PLAY_TYPE`) references `PLS_MultiTenant`.`PLAY_TYPE` (`PID`);

    CALL `AttachPlayTypes`();

    ALTER `PLS_MultiTenant`.`PLAY` Modify `FK_PLAY_TYPE` bigint NOT NULL;
  END;
//
DELIMITER;

CREATE PROCEDURE `CreateDropBoxTable`()
  BEGIN
    create table `ATLAS_DROPBOX` (
      `PID`              bigint not null auto_increment,
      `ACCESS_MODE`      varchar(20),
      `DROPBOX`          varchar(8),
      `EXTERNAL_ACCOUNT` varchar(20),
      `LATTICE_USER`     varchar(20),
      `FK_TENANT_ID`     bigint not null,
      primary key (`PID`)
    )
      engine = InnoDB;
    alter table `ATLAS_DROPBOX`
      add constraint UX_DROPBOX unique (`DROPBOX`);
    alter table `ATLAS_DROPBOX`
      add constraint `FK_ATLASDROPBOX_FKTENANTID_TENANT` foreign key (`FK_TENANT_ID`) references `TENANT` (`TENANT_PID`)
      on delete cascade;

  END;
//
DELIMITER;

CALL `UpdatePLSTables`();
CALL `CreateDropBoxTable`();

