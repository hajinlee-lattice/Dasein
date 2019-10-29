package com.latticeengines.domain.exposed.cdl.activity;

import com.latticeengines.domain.exposed.query.BusinessEntity;

/**
 * Information about a catalog/stream import
 */
public class ActivityImport {

    private BusinessEntity entity;
    private String uniqueId;
    private String tableName;
    private String originalFilename;

    public ActivityImport() {
    }

    public ActivityImport(BusinessEntity entity, String uniqueId, String tableName, String originalFilename) {
        this.entity = entity;
        this.uniqueId = uniqueId;
        this.tableName = tableName;
        this.originalFilename = originalFilename;
    }

    public BusinessEntity getEntity() {
        return entity;
    }

    public void setEntity(BusinessEntity entity) {
        this.entity = entity;
    }

    public void setUniqueId(String uniqueId) {
        this.uniqueId = uniqueId;
    }

    public String getUniqueId() {
        return uniqueId;
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
        return "ActivityImport{" + "entity=" + entity + ", uniqueId='" + uniqueId + '\'' + ", tableName='" + tableName
                + '\'' + ", originalFilename='" + originalFilename + '\'' + '}';
    }
}
