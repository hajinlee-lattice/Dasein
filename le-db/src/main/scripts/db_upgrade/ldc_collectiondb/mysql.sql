USE `LDC_CollectionDB`;

CREATE PROCEDURE `UpdateCollcetionDBTables`()
  BEGIN
  update `LDC_CollectionDB`.`VendorConfig` set DOMAIN_CHECK_FIELD = "Rank", DOMAIN_FIELD = "Domain" where VENDOR = "SEMRUSH"
  commit
  END;
//
DELIMITER;
