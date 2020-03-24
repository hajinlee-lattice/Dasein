USE `GlobalAuthentication`;

DROP PROCEDURE IF EXISTS `UpdateTable`;
DELIMITER //
  BEGIN
   CREATE PROCEDURE `UpdateTable`()
      alter table `SECURITY_IDENTITY_PROVIDER` drop index `ENTITY_ID`;
      alter table `SECURITY_IDENTITY_PROVIDER` add constraint `UX_IDENTITY_ID` unique (`TENANT_ID`, `ENTITY_ID`);

  END;
//
DELIMITER ;

CALL `UpdateTable`();
