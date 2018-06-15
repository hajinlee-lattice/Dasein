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
    ADD `MODELING_JOB_STATUS` INTEGER NOT NULL DEFAULT 0;

	ALTER TABLE `PLS_MultiTenant`.`RATING_ENGINE` ADD `LATEST_ITERATION` bigint;
	ALTER TABLE `PLS_MultiTenant`.`RATING_ENGINE` ADD CONSTRAINT `FK_RATINGENGINE_LATESTITERATION_RATINGMODEL` FOREIGN KEY (`LATEST_ITERATION`) REFERENCES `RATING_MODEL` (`PID`) ON DELETE CASCADE;
	UPDATE `PLS_MultiTenant`.`RATING_ENGINE` SET `LATEST_ITERATION` = `ACTIVE_MODEL_PID`;

	ALTER TABLE `PLS_MultiTenant`.`RATING_ENGINE` ADD `PUBLISHED_ITERATION` bigint;
	ALTER TABLE `PLS_MultiTenant`.`RATING_ENGINE` ADD CONSTRAINT `FK_RATINGENGINE_PUBLISHEDITERATION_RATINGMODEL` FOREIGN KEY (`PUBLISHED_ITERATION`) REFERENCES `RATING_MODEL` (`PID`) ON DELETE CASCADE;
	UPDATE `PLS_MultiTenant`.`RATING_ENGINE` SET `PUBLISHED_ITERATION` = `ACTIVE_MODEL_PID` WHERE `TYPE` = 'RULE_BASED' AND `STATUS`='ACTIVE';
	UPDATE `PLS_MultiTenant`.`RATING_ENGINE` re JOIN  `PLS_MultiTenant`.`RATING_MODEL` rm ON re.`PID` = rm.`FK_RATING_ENGINE_ID` JOIN `PLS_MultiTenant`.`AI_MODEL` am ON am.`PID` = rm.`PID`
		SET re.`PUBLISHED_ITERATION` = re.`ACTIVE_MODEL_PID`
		WHERE am.`MODEL_SUMMARY_ID` IS NOT NULL;

    ALTER TABLE `PLS_MultiTenant`.`RATING_ENGINE` ADD `SCORING_ITERATION` bigint;
    ALTER TABLE `PLS_MultiTenant`.`RATING_ENGINE` ADD CONSTRAINT `FK_RATINGENGINE_SCORINGITERATION_RATINGMODEL` FOREIGN KEY (`SCORING_ITERATION`) REFERENCES `RATING_MODEL` (`PID`) ON DELETE CASCADE;
    UPDATE `PLS_MultiTenant`.`RATING_ENGINE` SET `SCORING_ITERATION` = `ACTIVE_MODEL_PID` WHERE `TYPE` = 'RULE_BASED';
    UPDATE `PLS_MultiTenant`.`RATING_ENGINE` re JOIN  `PLS_MultiTenant`.`RATING_MODEL` rm ON re.`PID` = rm.`FK_RATING_ENGINE_ID` JOIN `PLS_MultiTenant`.`AI_MODEL` am ON am.`PID` = rm.`PID`
		SET `SCORING_ITERATION` = `ACTIVE_MODEL_PID`
		WHERE am.`MODEL_SUMMARY_ID` IS NOT NULL;

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

CREATE PROCEDURE `UpdateWorkflowJobUpdate`()
  BEGIN
    ALTER TABLE `PLS_MultiTenant`.`WORKFLOW_JOB_UPDATE`
      ADD `CREATE_TIME` BIGINT(20);
  END;
//
DELIMITER ;

CALL `UpdateSchema`();
CALL `UpdateWorkflowJobUpdate`();
