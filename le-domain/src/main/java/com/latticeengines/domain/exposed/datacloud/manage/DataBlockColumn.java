package com.latticeengines.domain.exposed.datacloud.manage;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;

// to be persist in DB
public class DataBlockColumn implements MetadataColumn {

    private String attrName;
    private String displayName;
    private String jsonPath;

    public DataBlockColumn(String attrName, String displayName, String jsonPath) {
        this.attrName = attrName;
        this.displayName = displayName;
        this.jsonPath = jsonPath;
    }

    public String getAttrName() {
        return attrName;
    }

    public void setAttrName(String attrName) {
        this.attrName = attrName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getJsonPath() {
        return jsonPath;
    }

    public void setJsonPath(String jsonPath) {
        this.jsonPath = jsonPath;
    }

    @Override
    public ColumnMetadata toColumnMetadata() {
        ColumnMetadata cm = new ColumnMetadata();
        cm.setAttrName(getColumnId());
        cm.setDisplayName(getDisplayName());
        cm.setJavaClass(String.class.getSimpleName());
        return cm;
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String getColumnId() {
        return attrName;
    }

    @Override
    public boolean containsTag(String tag) {
        return false;
    }

}
