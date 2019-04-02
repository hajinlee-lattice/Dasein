USE `DocumentDB`;

CREATE PROCEDURE `UpdateDocumentTables`()
  BEGIN
  DROP INDEX IX_NAMESPACE ON AttributeConfiguration;
  drop INDEX UX_NAME ON AttributeConfiguration;
  ALTER TABLE AttributeConfiguration drop COLUMN Entity;
  ALTER TABLE AttributeConfiguration ADD COLUMN Entity varchar(30) COLLATE utf8mb4_unicode_ci GENERATED ALWAYS AS (json_unquote(json_extract(`Document`,'$.Entity'))) VIRTUAL;
  create index IX_NAMESPACE on `AttributeConfiguration` (`TenantId`, `Entity`);
  alter table `AttributeConfiguration` add constraint UX_NAME unique (`TenantId`, `Entity`, `AttrName`);
  END;
//
DELIMITER;
