use GlobalAuthentication;

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
