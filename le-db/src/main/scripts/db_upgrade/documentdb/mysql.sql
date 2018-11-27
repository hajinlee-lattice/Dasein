USE `DocumentDB`;

CREATE PROCEDURE `UpdateDocumentTables`()
  BEGIN
  create table `ServiceConfiguration` (`PID` bigint not null auto_increment, `ServiceConfig` JSON, `ServiceName` varchar(255), `State` JSON, `TenantPId` bigint not null, primary key (`PID`)) engine=InnoDB;
  create table `TenantConfiguration` (`PID` bigint not null auto_increment, `ContractId` varchar(255), `ContractPropertities` JSON, `DefaultSpace` varchar(255), `FeatureFlags` JSON, `SpaceConfiguration` JSON, `SpaceProperties` JSON, `Status` varchar(255) not null, `TenantId` varchar(255), `TenantProperties` JSON, primary key (`PID`)) engine=InnoDB;
  alter table `TenantConfiguration` add constraint UX_NAME unique (`TenantId`, `ContractId`);
  alter table `ServiceConfiguration` add constraint `FK_ServiceConfiguration_TenantPId_TenantConfiguration` foreign key (`TenantPId`) references `TenantConfiguration` (`PID`) on delete cascade;

  END;
//
DELIMITER;
