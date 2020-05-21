package com.latticeengines.domain.exposed.pls;

import java.util.List;

import org.apache.commons.collections4.CollectionUtils;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.EntityType;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class DeleteActionConfiguration extends ActionConfiguration {

    @JsonProperty("delete_data_table")
    private String deleteDataTable;

    @JsonProperty("id_entity")
    private BusinessEntity idEntity;

    @JsonProperty("DeleteEntityType")
    private EntityType deleteEntityType;

    // if empty, means all entities
    @JsonProperty("delete_entities")
    private List<BusinessEntity> deleteEntities;

    // if empty, means all streams
    @JsonProperty("delete_stream_ids")
    private List<String> deleteStreamIds;

    @JsonProperty("IdSystem")
    private String idSystem;

    @JsonProperty("FromDate")
    private String fromDate;

    @JsonProperty("ToDate")
    private String toDate;

    public String getDeleteDataTable() {
        return deleteDataTable;
    }

    public void setDeleteDataTable(String deleteDataTable) {
        this.deleteDataTable = deleteDataTable;
    }

    public List<BusinessEntity> getDeleteEntities() {
        return deleteEntities;
    }

    public void setDeleteEntities(List<BusinessEntity> deleteEntities) {
        this.deleteEntities = deleteEntities;
    }

    public List<String> getDeleteStreamIds() {
        return deleteStreamIds;
    }

    public void setDeleteStreamIds(List<String> deleteStreamIds) {
        this.deleteStreamIds = deleteStreamIds;
    }

    public BusinessEntity getIdEntity() {
        return idEntity;
    }

    public void setIdEntity(BusinessEntity idEntity) {
        this.idEntity = idEntity;
    }

    public boolean hasEntity(BusinessEntity entity) {
        return CollectionUtils.isEmpty(getDeleteEntities()) || getDeleteEntities().contains(entity);
    }

    public boolean hasStream(String streamId) {
        return hasEntity(BusinessEntity.ActivityStream) && ( //
        CollectionUtils.isEmpty(getDeleteStreamIds()) || getDeleteStreamIds().contains(streamId) //
        );
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

    public String getFromDate() {
        return fromDate;
    }

    public void setFromDate(String fromDate) {
        this.fromDate = fromDate;
    }

    public String getToDate() {
        return toDate;
    }

    public void setToDate(String toDate) {
        this.toDate = toDate;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    @Override
    public String serialize() {
        return toString();
    }
}
