DROP SCHEMA IF EXISTS GlobalAuthentication;

CREATE SCHEMA IF NOT EXISTS GlobalAuthentication;

GRANT ALL ON GlobalAuthentication.* TO root@localhost;

USE `GlobalAuthentication`;

source WSHOME/le-db/ddl_globalauthentication_mysql5innodb.sql

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
  Last_Modified_by
) VALUES (
  'bnguyen@lattice-engines.com',
  'Super',
  'User',
  '000-000-0000',
  'Super User',
  1,
  NOW(),
  NOW(),
  0,
  0
), (
  'pls-super-admin-tester@test.lattice-engines.com',
  'Super',
  'User',
  '000-000-0000',
  'Super User',
  1,
  NOW(),
  NOW(),
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
 'bnguyen@lattice-engines.com',
  'mE2oR2b7hmeO1DpsoKuxhzx/7ODE9at6um7wFqa7udg=',
  0,
  NOW(),
  NOW(),
  0,
  0
FROM GlobalUser
WHERE Email = 'bnguyen@lattice-engines.com';

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
 'pls-super-admin-tester@test.lattice-engines.com',
  'EETAlfvFzCdm6/t3Ro8g89vzZo6EDCbucJMTPhYgWiE=',
  0,
  NOW(),
  NOW(),
  0,
  0
FROM GlobalUser
WHERE Email = 'pls-super-admin-tester@test.lattice-engines.com';


INSERT INTO GlobalUserTenantRight (
  Tenant_ID,
  User_ID,
  Operation_Name,
  Creation_Date,
  Last_Modification_Date,
  Created_By,
  Last_Modified_by
) SELECT
  u.GlobalUser_ID,
  t.GlobalTenant_ID,
  'SUPER_ADMIN',
  NOW(),
  NOW(),
  0,
  0
FROM GlobalUser u, GlobalTenant t
WHERE u.Email = 'bnguyen@lattice-engines.com'
AND t.Deployment_ID = 'LocalTest.LocalTest.Production'
LIMIT 1;

INSERT INTO GlobalUserTenantRight (
  Tenant_ID,
  User_ID,
  Operation_Name,
  Creation_Date,
  Last_Modification_Date,
  Created_By,
  Last_Modified_by
) SELECT
  u.GlobalUser_ID,
  t.GlobalTenant_ID,
  'SUPER_ADMIN',
  NOW(),
  NOW(),
  0,
  0
FROM GlobalUser u, GlobalTenant t
WHERE u.Email = 'pls-super-admin-tester@test.lattice-engines.com'
AND t.Deployment_ID = 'LocalTest.LocalTest.Production'
LIMIT 1;