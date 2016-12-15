package com.latticeengines.domain.exposed.datafabric;

import com.latticeengines.domain.exposed.dataplatform.HasId;

public class CompositeFabricEntity implements HasId<String> {

    private final String DELIMITER = "_";

    @DynamoHashKey(name = "parentKey")
    private String parentKey;
    @DynamoRangeKey(name = "entityId")
    private String entityId;

    public String getParentKey() {
        return parentKey;
    }

    public void setParentKey(String parentKey) {
        this.parentKey = parentKey;
    }

    public void setId(String parentID, String entityName, String entityId) {
        this.parentKey = parentID + DELIMITER + entityName;
        this.entityId = entityId;
    }

    public String getId() {
        return parentKey + "#" + entityId;
    }

    public void setId(String id) {

        String[] ids = id.split("#");
        if (ids.length != 2) {
            return;
        } else {
            parentKey = ids[0];
            entityId = ids[1];
        }
    }

    public String getParentId() {
        return getParentAttr(0);
    }

    public String getEntityName() {
        return getParentAttr(1);
    }

    public String getEntityId() {
        return entityId;
    }

    public void setEntityId(String entityId) {
        this.entityId = entityId;
    }

    private String getParentAttr(int i) {
        String[] ids = parentKey.split(DELIMITER);
        if (i >= ids.length) {
            return null;
        } else {
            return ids[i];
        }
    }
}
