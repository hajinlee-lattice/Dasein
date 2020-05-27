package com.latticeengines.domain.exposed.serviceflows.cdl.steps.match;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.workflow.BaseWrapperStepConfiguration;

public class RenameAndMatchStepConfiguration extends BaseWrapperStepConfiguration {
    @JsonProperty("table_name")
    private String tableName;

    @JsonProperty("IdEntity")
    private BusinessEntity idEntity;

    @JsonProperty("DeleteEntityType")
    private EntityType deleteEntityType;

    @JsonProperty("IdSystem")
    private String idSystem;

    @JsonProperty("DeleteActionPid")
    private String deleteActionPid;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public BusinessEntity getIdEntity() {
        return idEntity;
    }

    public void setIdEntity(BusinessEntity idEntity) {
        this.idEntity = idEntity;
    }

    public EntityType getDeleteEntityType() {
        return deleteEntityType;
    }

    public void setDeleteEntityType(EntityType deleteEntityType) {
        this.deleteEntityType = deleteEntityType;
    }

    public String getIdSystem() {
        return idSystem;
    }

    public void setIdSystem(String idSystem) {
        this.idSystem = idSystem;
    }

    public String getDeleteActionPid() {
        return deleteActionPid;
    }

    public void setDeleteActionPid(String deleteActionPid) {
        this.deleteActionPid = deleteActionPid;
    }
}
