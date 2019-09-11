package com.latticeengines.domain.exposed.cdl.activity;

/**
 * Information about a catalog import
 */
public class CatalogImport {

    private String catalogName;
    private String tableName;
    private String originalFilename;

    public CatalogImport() {
    }

    public CatalogImport(String catalogName, String tableName, String originalFilename) {
        this.catalogName = catalogName;
        this.tableName = tableName;
        this.originalFilename = originalFilename;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public void setCatalogName(String catalogName) {
        this.catalogName = catalogName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getOriginalFilename() {
        return originalFilename;
    }

    public void setOriginalFilename(String originalFilename) {
        this.originalFilename = originalFilename;
    }

    @Override
    public String toString() {
        return "CatalogImport{" + "catalogName='" + catalogName + '\'' + ", tableName='" + tableName + '\''
                + ", originalFilename='" + originalFilename + '\'' + '}';
    }
}
