CREATE PROCEDURE `UpdateCDLTables`()
  BEGIN
    ALTER TABLE `PLS_MultiTenant`.`PLAY`
    ADD `DELETED` bit null DEFAULT 0;

    ALTER TABLE `PLS_MultiTenant`.`PLAY`
    ADD `CLEANUP_DONE` bit null DEFAULT 0;

    CREATE INDEX PLAY_DELETED ON `PLS_MultiTenant`.`PLAY` (`DELETED`);
    CREATE INDEX PLAY_CLEANUP_DONE ON `PLS_MultiTenant`.`PLAY` (`CLEANUP_DONE`);

    UPDATE `PLS_MultiTenant`.`PLAY`
    SET `DELETED` = 0;

    UPDATE `PLS_MultiTenant`.`PLAY`
    SET `CLEANUP_DONE` = 0;

    ALTER TABLE `PLS_MultiTenant`.`PLAY_LAUNCH`
    ADD `DELETED` bit null DEFAULT 0;

    CREATE INDEX PLAY_LAUNCH_DELETED ON `PLS_MultiTenant`.`PLAY_LAUNCH` (`DELETED`);

    UPDATE `PLS_MultiTenant`.`PLAY_LAUNCH`
    SET `DELETED` = 0;

    ALTER TABLE `PLS_MultiTenant`.`AI_MODEL`
    ADD `MODELING_JOB_STATUS` integer not null DEFAULT 0;
  END;
//
DELIMITER ;

DELIMITER //
CREATE PROCEDURE `UpdateSchema`()
    BEGIN
        START TRANSACTION;
        CALL `UpdateCDLTables`();
        COMMIT;
    END;
//
DELIMITER ;

CALL `UpdateSchema`();
