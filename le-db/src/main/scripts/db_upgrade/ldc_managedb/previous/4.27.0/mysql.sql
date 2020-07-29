USE `LDC_ManageDB`;

SET SQL_SAFE_UPDATES = 0;

DROP PROCEDURE IF EXISTS `UpdateSchema`;

DELIMITER //
CREATE PROCEDURE `UpdateMatchBlockTable`()
BEGIN
    # add column if not exists
    IF NOT EXISTS(SELECT *
                  FROM information_schema.COLUMNS
                  WHERE TABLE_SCHEMA = 'LDC_ManageDB'
                    AND TABLE_NAME = 'MatchBlock'
                    AND COLUMN_NAME = 'NewEntityCounts')
    THEN
        ALTER TABLE MatchBlock
            ADD COLUMN NewEntityCounts JSON DEFAULT NULL;
    END IF;
END //
DELIMITER ;

DELIMITER //
CREATE PROCEDURE `UpdateSchema`()
BEGIN
    CALL `UpdateMatchBlockTable`();
END //
DELIMITER ;

CALL `UpdateSchema`();
