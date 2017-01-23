USE `LDC_ManageDB`;

DROP PROCEDURE IF EXISTS `UpdateSchema`;

DELIMITER //
CREATE PROCEDURE `UpdateSchema`()
  BEGIN
    IF NOT EXISTS(SELECT table_name
                  FROM INFORMATION_SCHEMA.TABLES
                  WHERE table_schema = 'LDC_ManageDB'
                        AND table_name = 'CategoricalAttribute')
    THEN
      CREATE TABLE `CategoricalAttribute` (
        `PID`       BIGINT       NOT NULL AUTO_INCREMENT UNIQUE,
        `AttrName`  VARCHAR(100) NOT NULL,
        `AttrValue` VARCHAR(500) NOT NULL,
        `ParentID`  BIGINT,
        PRIMARY KEY (`PID`),
        UNIQUE (`AttrName`, `AttrValue`, `ParentID`)
      )
        ENGINE = InnoDB;
    END IF;

    IF NOT EXISTS(SELECT table_name
                  FROM INFORMATION_SCHEMA.TABLES
                  WHERE table_schema = 'LDC_ManageDB'
                        AND table_name = 'AccountMasterFact')
    THEN
      CREATE TABLE `AccountMasterFact` (
        `PID`         BIGINT   NOT NULL AUTO_INCREMENT UNIQUE,
        `Category`    BIGINT   NOT NULL,
        `EncodedCube` LONGTEXT NOT NULL,
        `Industry`    BIGINT   NOT NULL,
        `Location`    BIGINT   NOT NULL,
        `NumEmpRange` BIGINT   NOT NULL,
        `NumLocRange` BIGINT   NOT NULL,
        `RevRange`    BIGINT   NOT NULL,
        PRIMARY KEY (`PID`),
        UNIQUE (`Location`, `Industry`, `NumEmpRange`, `RevRange`, `NumLocRange`, `Category`)
      )
        ENGINE = InnoDB;
    END IF;

    IF NOT EXISTS(SELECT TABLE_NAME
                  FROM INFORMATION_SCHEMA.TABLES
                  WHERE table_schema = 'LDC_ManageDB'
                        AND TABLE_NAME = 'CategoricalDimension')
    THEN
      CREATE TABLE `CategoricalDimension` (
        `PID`        BIGINT       NOT NULL AUTO_INCREMENT UNIQUE,
        `Dimension`  VARCHAR(100) NOT NULL,
        `RootAttrId` BIGINT       NOT NULL UNIQUE,
        `Source`     VARCHAR(100) NOT NULL,
        PRIMARY KEY (`PID`),
        UNIQUE (`Source`, `Dimension`)
      )
        ENGINE = InnoDB;
    END IF;

    IF EXISTS(SELECT TABLE_NAME
              FROM INFORMATION_SCHEMA.STATISTICS
              WHERE table_schema = 'LDC_ManageDB'
                    AND TABLE_NAME = 'CategoricalAttribute'
                    AND index_name = 'IX_PARENT_ID')
    THEN
      DROP INDEX IX_PARENT_ID
      ON `CategoricalAttribute`;
    END IF;

    CREATE INDEX IX_PARENT_ID
      ON `CategoricalAttribute` (`ParentID`);

    IF EXISTS(SELECT TABLE_NAME
              FROM INFORMATION_SCHEMA.STATISTICS
              WHERE table_schema = 'LDC_ManageDB'
                    AND TABLE_NAME = 'AccountMasterFact'
                    AND index_name = 'IX_DIMENSIONS')
    THEN
      DROP INDEX IX_DIMENSIONS
      ON `AccountMasterFact`;
    END IF;

    CREATE INDEX IX_DIMENSIONS
      ON `AccountMasterFact` (`Category`, `Industry`, `Location`, `NumEmpRange`, `NumLocRange`, `RevRange`);

    IF EXISTS(SELECT TABLE_NAME
              FROM INFORMATION_SCHEMA.STATISTICS
              WHERE table_schema = 'LDC_ManageDB'
                    AND TABLE_NAME = 'CategoricalDimension'
                    AND index_name = 'IX_SOURCE_DIMENSION')
    THEN
      DROP INDEX IX_SOURCE_DIMENSION
      ON `CategoricalDimension`;
    END IF;

    CREATE INDEX IX_SOURCE_DIMENSION
      ON `CategoricalDimension` (`Dimension`, `Source`);

  END //
DELIMITER ;

CALL `UpdateSchema`();



