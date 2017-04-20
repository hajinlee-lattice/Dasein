



drop table if exists `GlobalAuthentication`;
drop table if exists `GlobalSession`;
drop table if exists `GlobalTenant`;
drop table if exists `GlobalTicket`;
drop table if exists `GlobalUserTenantRight`;
drop table if exists `GlobalUser`;
drop table if exists `SECURITY_IDENTITY_PROVIDER`;
create table `GlobalAuthentication` (`GlobalAuthentication_ID` bigint not null auto_increment, `Created_By` integer not null, `Creation_Date` datetime not null, `Last_Modification_Date` datetime not null, `Last_Modified_By` integer not null, `MustChangePassword` boolean not null, `Password` varchar(255) not null, `Username` varchar(255) not null, User_ID bigint not null, primary key (`GlobalAuthentication_ID`)) ENGINE=InnoDB;
create table `GlobalSession` (`GlobalSession_ID` bigint not null auto_increment, `Created_By` integer not null, `Creation_Date` datetime not null, `Last_Modification_Date` datetime not null, `Last_Modified_By` integer not null, `Tenant_ID` bigint not null, `Ticket_ID` bigint not null, `User_ID` bigint not null, primary key (`GlobalSession_ID`)) ENGINE=InnoDB;
create table `GlobalTenant` (`GlobalTenant_ID` bigint not null auto_increment unique, `Created_By` integer not null, `Creation_Date` datetime not null, `Last_Modification_Date` datetime not null, `Last_Modified_By` integer not null, `Deployment_ID` varchar(255) unique, `Display_Name` varchar(255), primary key (`GlobalTenant_ID`)) ENGINE=InnoDB;
create table `GlobalTicket` (`GlobalTicket_ID` bigint not null auto_increment unique, `Created_By` integer not null, `Creation_Date` datetime not null, `Last_Modification_Date` datetime not null, `Last_Modified_By` integer not null, `Last_Access_Date` datetime not null, `Ticket` varchar(255) not null, `User_ID` bigint not null, primary key (`GlobalTicket_ID`)) ENGINE=InnoDB;
create table `GlobalUserTenantRight` (`GlobalUserTenantRight_ID` bigint not null auto_increment unique, `Created_By` integer not null, `Creation_Date` datetime not null, `Last_Modification_Date` datetime not null, `Last_Modified_By` integer not null, `Operation_Name` varchar(255), Tenant_ID bigint not null, User_ID bigint not null, primary key (`GlobalUserTenantRight_ID`)) ENGINE=InnoDB;
create table `GlobalUser` (`GlobalUser_ID` bigint not null auto_increment unique, `Created_By` integer not null, `Creation_Date` datetime not null, `Last_Modification_Date` datetime not null, `Last_Modified_By` integer not null, `Email` varchar(255), `First_Name` varchar(255), `IsActive` boolean, `Last_Name` varchar(255), `Phone_Number` varchar(255), `Title` varchar(255), primary key (`GlobalUser_ID`)) ENGINE=InnoDB;
create table `SECURITY_IDENTITY_PROVIDER` (`PID` bigint not null auto_increment unique, `CREATED` datetime not null, `ENTITY_ID` varchar(255) not null unique, `METADATA` longtext not null, `UPDATED` datetime not null, TENANT_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;

