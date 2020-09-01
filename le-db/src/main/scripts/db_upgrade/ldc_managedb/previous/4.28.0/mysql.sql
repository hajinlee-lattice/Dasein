/*
* script name - mysql.sql
* purpose - Base sql file to prepare DB upgrade script.
* It contains DDL/DML sql queries that can be applied at 'release regression' & 'release window' cycle.
* Ensure to maintain backward compatibility.
*/

USE `LDC_ManageDB`;

drop procedure IF EXISTS `UpdateSchema`;
DELIMITER //

-- ##############################################################
create procedure `UpdateSchema`()
  BEGIN
      -- User input section (DDL/DML). This is just a template, developer can modify based on need.
        CREATE TABLE `ContactMasterTpsColumn` (
            `PID`               BIGINT NOT NULL AUTO_INCREMENT,
            `ColumnName`        VARCHAR(100) NOT NULL,
            `JavaClass`         VARCHAR(50) NOT NULL,
            `IsDerived`         BOOLEAN NOT NULL,
            `MatchDestination`  VARCHAR(100),
            PRIMARY KEY (`PID`)
        ) ENGINE = InnoDB;

        CREATE INDEX IX_ColumnName ON `ContactMasterTpsColumn` (`ColumnName`);

        CREATE INDEX IX_MatchDestination ON `ContactMasterTpsColumn` (`MatchDestination`);

        INSERT INTO `LDC_ManageDB`.`Publication`(`CronExpression`, `DestinationConfig`,
                                                 `MaterialType`, `NewJobMaxRetry`,
                                                 `NewJobRetryInterval`, `PublicationName`,
                                                 `PublicationType`, `SchedularEnabled`, `SourceName`)
                    VALUES
                    (null, '{"ConfigurationType":"PublishToDynamoConfiguration","Strategy":"REPLACE","Alias":"Production","EntityClass":"com.latticeengines.domain.exposed.datacloud.match.ContactTpsEntry","RecordType":"ContactTpsEntry","RuntimeReadCapacity":1500,"RuntimeWriteCapacity":5,"LoadingReadCapacity":0,"LoadingWriteCapacity":0,"CustomerCMK":"9e245e78-530d-4a5b-9043-b535a39c4c1d"}',
                    'SOURCE', 0, null, 'CMTpsSourceRebuildProd', 'DYNAMO', 0, 'ContactMasterTPSSeed');

        INSERT INTO `LDC_ManageDB`.`Publication`(`CronExpression`, `DestinationConfig`,
                                                 `MaterialType`, `NewJobMaxRetry`,
                                                 `NewJobRetryInterval`, `PublicationName`,
                                                 `PublicationType`, `SchedularEnabled`, `SourceName`)
                    VALUES
                    (null, '{"ConfigurationType":"PublishToDynamoConfiguration","Strategy":"REPLACE","Alias":"Production","EntityClass":"com.latticeengines.domain.exposed.datacloud.match.ContactTpsLookupEntry","RecordType":"ContactTpsLookupEntry","RuntimeReadCapacity":1500,"RuntimeWriteCapacity":5,"LoadingReadCapacity":0,"LoadingWriteCapacity":0,"CustomerCMK":"9e245e78-530d-4a5b-9043-b535a39c4c1d"}',
                    'SOURCE', 0, null, 'CMTpsLookupRebuildProd', 'DYNAMO', 0, 'ContactMasterTpsLookup');
  END //
-- ##############################################################

-- DO NOT touch this part
DELIMITER ;
CALL `UpdateSchema`();
