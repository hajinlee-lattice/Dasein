use GlobalAuthentication;

CREATE TABLE GlobalTenant (
	GlobalTenant_ID bigint NOT NULL auto_increment UNIQUE,
	Deployment_ID nvarchar(255) NULL,
	Display_Name nvarchar(255) NULL,
	Creation_Date datetime NOT NULL,
	Last_Modification_Date datetime NOT NULL,
	Created_By int DEFAULT 0,
	Last_Modified_By int DEFAULT 0,
	PRIMARY KEY (GlobalTenant_ID) 
)ENGINE=InnoDB;

CREATE TABLE GlobalUser(
	GlobalUser_ID bigint NOT NULL auto_increment UNIQUE,
	Email nvarchar(255) NULL,
	First_Name nvarchar(255) NULL,
	Last_Name nvarchar(255) NULL,
	Title nvarchar(255) NULL,
	Phone_Number nvarchar(255) NULL,
	IsActive bit NOT NULL,
	Creation_Date datetime NOT NULL,
	Last_Modification_Date datetime NOT NULL,
	Created_By int DEFAULT 0,
	Last_Modified_By int DEFAULT 0,
	PRIMARY KEY (GlobalUser_ID) 
)ENGINE=InnoDB;

CREATE TABLE GlobalTicket(
	GlobalTicket_ID bigint NOT NULL auto_increment UNIQUE,
	Ticket nvarchar(255) NOT NULL,
	User_ID bigint NOT NULL,
	Last_Access_Date datetime NOT NULL,
	Creation_Date datetime NOT NULL,
	Last_Modification_Date datetime NOT NULL,
	Created_By int DEFAULT 0,
	Last_Modified_By int DEFAULT 0,
	PRIMARY KEY (GlobalTicket_ID),
	FOREIGN KEY (User_ID)
		REFERENCES GlobalUser(GlobalUser_ID)
		ON DELETE CASCADE
       		ON UPDATE CASCADE
)ENGINE=InnoDB;

CREATE TABLE GlobalAuthentication(
	GlobalAuthentication_ID bigint NOT NULL auto_increment UNIQUE,
	User_ID bigint NOT NULL,
	Username nvarchar(255) NOT NULL,
	Password nvarchar(255) NOT NULL,
	MustChangePassword bit NOT NULL,
	Creation_Date datetime NOT NULL,
	Last_Modification_Date datetime NOT NULL,
	Created_By int DEFAULT 0,
	Last_Modified_By int DEFAULT 0,
	PRIMARY KEY (GlobalAuthentication_ID),
	FOREIGN KEY (User_ID)
		REFERENCES GlobalUser(GlobalUser_ID)
		ON DELETE CASCADE
       		ON UPDATE CASCADE
)ENGINE=InnoDB;

CREATE TABLE GlobalSession (
	GlobalSession_ID bigint NOT NULL auto_increment UNIQUE,
	Ticket_ID bigint NOT NULL,
	User_ID bigint NOT NULL,
	Tenant_ID bigint NOT NULL,
	Creation_Date datetime NOT NULL,
	Last_Modification_Date datetime NOT NULL,
	Created_By int DEFAULT 0,
	Last_Modified_By int DEFAULT 0,
	PRIMARY KEY (GlobalSession_ID),
	FOREIGN KEY (Tenant_ID)
		REFERENCES GlobalTenant(GlobalTenant_ID)
		ON DELETE CASCADE
       		ON UPDATE CASCADE,
	FOREIGN KEY (User_ID)
		REFERENCES GlobalUser(GlobalUser_ID)
		ON DELETE CASCADE
       		ON UPDATE CASCADE,
	FOREIGN KEY (Ticket_ID)
		REFERENCES GlobalTicket(GlobalTicket_ID)
		ON DELETE CASCADE
       		ON UPDATE CASCADE
)ENGINE=InnoDB;

CREATE TABLE GlobalUserTenantRight(
	GlobalUserTenantRight_ID bigint NOT NULL auto_increment UNIQUE,
	User_ID bigint NOT NULL,
	Tenant_ID bigint NOT NULL,
	Operation_Name nvarchar(255) NULL,
	Creation_Date datetime NOT NULL,
	Last_Modification_Date datetime NOT NULL,
	Created_By int DEFAULT 0,
	Last_Modified_By int DEFAULT 0,
	PRIMARY KEY (GlobalUserTenantRight_ID),
	FOREIGN KEY (Tenant_ID)
		REFERENCES GlobalTenant(GlobalTenant_ID)
		ON DELETE CASCADE
       		ON UPDATE CASCADE,
	FOREIGN KEY (User_ID)
		REFERENCES GlobalUser(GlobalUser_ID)
		ON DELETE CASCADE
       		ON UPDATE CASCADE
)ENGINE=InnoDB;

INSERT INTO GlobalTenant 
	(Deployment_ID, Display_Name
	,Creation_Date
	,Last_Modification_Date) 
 VALUES ('tenant1', 'TenantTest1', NOW(),NOW());

INSERT INTO GlobalTenant 
	(Deployment_ID
	,Display_Name
	,Creation_Date
	,Last_Modification_Date) 
 VALUES ('tenant2', 'TenantTest2', NOW(),NOW());

INSERT INTO GlobalUser 
	(Email
	,First_Name
	,Last_Name
	,Title, Phone_Number
	,IsActive
	,Creation_Date
	,Last_Modification_Date)
 VALUES ('bnguyen@lattice-engines.com', 'bngu', 'yen', 'Manager', '123456123', 1, NOW(),NOW());

INSERT INTO GlobalAuthentication 
	(User_ID
	,Username
	,Password
	,MustChangePassword
	,Creation_Date
	,Last_Modification_Date) 
 VALUES ((SELECT GlobalUser_ID from GlobalUser WHERE Email='bnguyen@lattice-engines.com'), 'bnguyen@lattice-engines.com', 'mE2oR2b7hmeO1DpsoKuxhzx/7ODE9at6um7wFqa7udg=', 0, NOW(),NOW());

INSERT INTO GlobalUserTenantRight
	(User_ID
	,Tenant_ID
	,Operation_Name
	,Creation_Date
	,Last_Modification_Date)
 VALUES ((SELECT GlobalUser_ID from GlobalUser WHERE Email='bnguyen@lattice-engines.com'), (SELECT GlobalTenant_ID from GlobalTenant WHERE Deployment_ID='tenant1'), 'TEST_ADMIN', NOW(),NOW());

INSERT INTO GlobalUserTenantRight
	(User_ID
	,Tenant_ID
	,Operation_Name
	,Creation_Date
	,Last_Modification_Date)
 VALUES ((SELECT GlobalUser_ID from GlobalUser WHERE Email='bnguyen@lattice-engines.com'), (SELECT GlobalTenant_ID from GlobalTenant WHERE Deployment_ID='tenant2'), 'TEST_ADMIN', NOW(),NOW());
