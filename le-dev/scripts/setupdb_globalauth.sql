DROP SCHEMA IF EXISTS GlobalAuthentication;

CREATE SCHEMA IF NOT EXISTS GlobalAuthentication;

GRANT ALL ON GlobalAuthentication.* TO root@localhost;

ALTER DATABASE GlobalAuthentication CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci;

USE `GlobalAuthentication`;

source WSHOME/ddl_globalauthentication_mysql5innodb.sql

INSERT INTO GlobalTenant (
  Deployment_ID,
  Display_Name,
  Creation_Date,
  Last_Modification_Date,
  Created_By,
  Last_Modified_by
) VALUES (
  'LocalTest.LocalTest.Production',
  'LocalTest',
  NOW(),
  NOW(),
  0,
  0
), (
'LocalTest2.LocalTest2.Production',
'LocalTest2',
NOW(),
NOW(),
0,
0
);

INSERT INTO GlobalUser (
  Email,
  First_Name,
  Last_Name,
  Phone_Number,
  Title,
  IsActive,
  Creation_Date,
  Last_Modification_Date,
  Created_By,
  Last_Modified_by,
  InvalidLoginAttempts
) VALUES (
  'ga_dev@lattice-engines.com',
  'Super',
  'User',
  '000-000-0000',
  'Super User',
  1,
  NOW(),
  NOW(),
  0,
  0,
  0
), (
  'pls-super-admin-tester@lattice-engines.com',
  'Super',
  'User',
  '000-000-0000',
  'Super User',
  1,
  NOW(),
  NOW(),
  0,
  0,
  0
);

INSERT INTO GlobalAuthentication (
  User_ID,
  Username,
  Password,
  MustChangePassword,
  Creation_Date,
  Last_Modification_Date,
  Created_By,
  Last_Modified_by
) SELECT
  GlobalUser_ID,
  Email,
  'EETAlfvFzCdm6/t3Ro8g89vzZo6EDCbucJMTPhYgWiE=',
  0,
  NOW(),
  NOW(),
  0,
  0
FROM GlobalUser
WHERE Email = 'pls-super-admin-tester@lattice-engines.com';

INSERT INTO GlobalAuthentication (
  User_ID,
  Username,
  Password,
  MustChangePassword,
  Creation_Date,
  Last_Modification_Date,
  Created_By,
  Last_Modified_by
) SELECT
  GlobalUser_ID,
  Email,
  '0eKk6IkISySyu3c2PiWh18U/KGh+FW9+99QC014FwXc=',
  0,
  NOW(),
  NOW(),
  0,
  0
FROM GlobalUser
WHERE Email = 'ga_dev@lattice-engines.com';

INSERT INTO GlobalUserTenantRight (
  Tenant_ID,
  User_ID,
  Operation_Name,
  Creation_Date,
  Last_Modification_Date,
  Created_By,
  Last_Modified_by
) SELECT
  t.GlobalTenant_ID,
  u.GlobalUser_ID,
  'SUPER_ADMIN',
  NOW(),
  NOW(),
  0,
  0
FROM GlobalUser u, GlobalTenant t
WHERE u.Email IN ('ga_dev@lattice-engines.com', 'pls-super-admin-tester@lattice-engines.com')
AND t.Deployment_ID IN ('LocalTest.LocalTest.Production', 'LocalTest2.LocalTest2.Production');
