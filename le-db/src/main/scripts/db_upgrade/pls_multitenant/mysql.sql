USE `PLS_MultiTenant`;

CREATE PROCEDURE `CreatePlayTypes`()
BEGIN
  DECLARE tenantId INT;
  DECLARE playTypeCount INT;
  DECLARE finished INT DEFAULT 0;
  DECLARE tenants TEXT;
  DECLARE curs CURSOR FOR  SELECT distinct FK_TENANT_ID FROM PLS_MultiTenant.PLAY;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET finished=1;

  OPEN curs;
  WHILE NOT finished DO
    FETCH curs INTO tenantId;
	Select count(PID) INTO playTypeCount FROM PLS_MultiTenant.PLAY_TYPE WHERE FK_TENANT_ID = TenantId;
    IF (playTypeCount < 1) THEN
       INSERT INTO `PLS_MultiTenant`.`PLAY_TYPE`
		(`CREATED`, `CREATED_BY`, `DISPLAY_NAME`, `ID`, `UPDATED`, `UPDATED_BY`, `FK_TENANT_ID`, `TENANT_ID`)
		VALUES
		(NOW(), 'build-admin@lattice-engines.com', 'List', UUID(), NOW(), 'build-admin@lattice-engines.com', tenantId, tenantId),
		(NOW(), 'build-admin@lattice-engines.com', 'Cross-Sell', UUID(), NOW(), 'build-admin@lattice-engines.com', tenantId, tenantId),
		(NOW(), 'build-admin@lattice-engines.com', 'Prospecting', UUID(), NOW(), 'build-admin@lattice-engines.com', tenantId, tenantId),
		(NOW(), 'build-admin@lattice-engines.com', 'Renewal', UUID(), NOW(), 'build-admin@lattice-engines.com', tenantId, tenantId),
		(NOW(), 'build-admin@lattice-engines.com', 'Upsell', UUID(), NOW(), 'build-admin@lattice-engines.com', tenantId, tenantId);
		SET tenants = concat(tenants , tenantId, ', ');
    END IF;
  END WHILE;

  CLOSE curs;
END
//
DELIMITER;

CREATE PROCEDURE `AttachPlayTypes`()
BEGIN
  DECLARE tenantId INT;
  DECLARE playId INT;
  DECLARE finished INT DEFAULT 0;
  DECLARE ratingEngineType VARCHAR(255);

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
    ALTER TABLE `PLS_MultiTenant`.`ACTION` ADD COLUMN `TRACKING_PID` bigint;
    ALTER TABLE `PLS_MultiTenant`.`TENANT` ADD COLUMN `CONTRACT` VARCHAR(255) DEFAULT NULL;
     UPDATE `PLS_MultiTenant`.`TENANT` set STATUS='ACTIVE' WHERE STATUS is NULL;
    ALTER TABLE `PLS_MultiTenant`.`TENANT` MODIFY `STATUS` VARCHAR(255) NOT NULL;

     UPDATE `PLS_MultiTenant`.`TENANT` set TENANT_TYPE = 'CUSTOMER' WHERE TENANT_TYPE is NULL;
    ALTER TABLE `PLS_MultiTenant`.`TENANT` MODIFY `TENANT_TYPE` VARCHAR(255) NOT NULL;

    ALTER TABLE `PLS_MultiTenant`.`PLAY` MODIFY COLUMN `DESCRIPTION` VARCHAR(255) DEFAULT NULL;

    CREATE TABLE `PLS_MultiTenant`.`PLAY_TYPE` (
    `PID` bigint NOT NULL auto_increment,
    `CREATED` datetime NOT NULL,
    `CREATED_BY` VARCHAR(255) NOT NULL,
    `DESCRIPTION` VARCHAR(8192),
    `DISPLAY_NAME` VARCHAR(255) NOT NULL,
    `ID` VARCHAR(255) NOT NULL,
    `TENANT_ID` bigint NOT NULL,
    `UPDATED` datetime NOT NULL,
    `UPDATED_BY` VARCHAR(255) NOT NULL,
    `FK_TENANT_ID` bigint NOT NULL,
    PRIMARY KEY (`PID`))
    engine=InnoDB;

    ALTER TABLE `PLS_MultiTenant`.`PLAY_TYPE` ADD CONSTRAINT `FK_PLAYTYPE_FKTENANTID_TENANT` FOREIGN KEY (`FK_TENANT_ID`) REFERENCES `PLS_MultiTenant`.`TENANT` (`TENANT_PID`) ON DELETE CASCADE;

    CALL `CreatePlayTypes`();

    ALTER TABLE `PLS_MultiTenant`.`PLAY` ADD COLUMN `FK_PLAY_TYPE` bigint;
    
    update PLS_MultiTenant.ACTION as a
    left join PLS_MultiTenant.WORKFLOW_JOB as w
    on a.tracking_id = w.workflow_id
    set a.TRACKING_PID = w.pid
    where a.TRACKING_PID is null

    ALTER TABLE `PLS_MultiTenant`.`PLAY` ADD CONSTRAINT `FK_PLAY_FKPLAYTYPE_PLAYTYPE` FOREIGN KEY (`FK_PLAY_TYPE`) REFERENCES `PLAY_TYPE` (`PID`) ON DELETE CASCADE;
    ALTER TABLE `PLS_MultiTenant`.`PLAY` ADD CONSTRAINT `FK_PLAY_FKPLAYTYPE_PLAYTYPE` FOREIGN KEY (`FK_PLAY_TYPE`) REFERENCES `PLS_MultiTenant`.`PLAY_TYPE` (`PID`);
    ALTER TABLE `PLS_MultiTenant`.`PLAY` ADD COLUMN `UPDATED_BY` VARCHAR(255) NOT NULL DEFAULT 'placeholderForUpdate';
    UPDATE `PLS_MultiTenant`.`PLAY` SET UPDATED_BY = CREATED_BY WHERE UPDATED_BY = 'placeholderForUpdate';
    ALTER TABLE `PLS_MultiTenant`.`PLAY_LAUNCH` ADD COLUMN `CREATED_BY` VARCHAR(255) NOT NULL DEFAULT 'placeholderForUpdate';
    ALTER TABLE `PLS_MultiTenant`.`PLAY_LAUNCH` ADD COLUMN `UPDATED_BY` VARCHAR(255) NOT NULL DEFAULT 'placeholderForUpdate';
    UPDATE `PLS_MultiTenant`.`PLAY_LAUNCH` pl, `PLS_MultiTenant`.`PLAY` p
    SET pl.UPDATED_BY = p.UPDATED_BY, pl.CREATED_BY = p.CREATED_BY
    WHERE pl.FK_PLAY_ID = p.PID;

    CALL `AttachPlayTypes`();

    -- IMPORTANT NOTE: Following ddl should only be executed after the active stack switches to the M23 release candidate
    ALTER `PLS_MultiTenant`.`PLAY` Modify `FK_PLAY_TYPE` bigint NOT NULL;

  END;
//
DELIMITER;

CREATE PROCEDURE `CreateDropBoxTable`()
  BEGIN
    CREATE TABLE `ATLAS_DROPBOX` (
      `PID`              bigint NOT NULL auto_increment,
      `ACCESS_MODE`      VARCHAR(20),
      `DROPBOX`          VARCHAR(8),
      `EXTERNAL_ACCOUNT` VARCHAR(20),
      `LATTICE_USER`     VARCHAR(20),
      `FK_TENANT_ID`     bigint NOT NULL,
      PRIMARY KEY (`PID`)
    )
      engine = InnoDB;
    ALTER TABLE `ATLAS_DROPBOX`
      ADD CONSTRAINT UX_DROPBOX UNIQUE (`DROPBOX`);
    ALTER TABLE `ATLAS_DROPBOX`
      ADD CONSTRAINT `FK_ATLASDROPBOX_FKTENANTID_TENANT` FOREIGN KEY (`FK_TENANT_ID`) REFERENCES `TENANT` (`TENANT_PID`)
      ON DELETE CASCADE;

  END;
//
DELIMITER;

CALL `UpdatePLSTables`();
CALL `CreateDropBoxTable`();

