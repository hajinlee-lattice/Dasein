package com.latticeengines.domain.exposed.cdl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.EntityType;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect( //
        fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE //
)
public class DeleteRequest {

    @JsonProperty("User")
    private String user;

    @JsonProperty("FileName")
    private String filename;

    // user provided IDs in delete file belongs to which entity
    @JsonProperty("IdEntity")
    private BusinessEntity idEntity;

    // user provided IDs in delete file should map to which system's ID
    @JsonProperty("IdSystem")
    private String idSystem;

    // entities to be deleted
    @JsonProperty("DeleteEntities")
    private List<BusinessEntity> deleteEntities;

    /**
     * specific activity streams to delete when deleteEntities contains
     * {@link BusinessEntity#ActivityStream}
     */
    @JsonProperty("DeleteStreamIds")
    private List<String> deleteStreamIds;

    /**
     * specific entity to be deleted. if provided, this field will be translated to
     * {@link this#deleteEntities} and {@link this#deleteStreamIds}
     */
    @JsonProperty("DeleteEntityType")
    private EntityType deleteEntityType;

    @JsonProperty("HardDelete")
    private Boolean hardDelete;

    /*-
     * If provided, only records within [ fromDate, toDate ] will be deleted.
     * These fields only apply to time series entities and will be ignored for other entities.
     */
    @JsonProperty("FromDate")
    private String fromDate;

    @JsonProperty("ToDate")
    private String toDate;

    @JsonIgnore
    private SourceFile sourceFile;

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public Boolean getHardDelete() {
        return hardDelete;
    }

    public void setHardDelete(Boolean hardDelete) {
        this.hardDelete = hardDelete;
    }

    public BusinessEntity getIdEntity() {
        return idEntity;
    }

    public void setIdEntity(BusinessEntity idEntity) {
        this.idEntity = idEntity;
    }

    public String getIdSystem() {
        return idSystem;
    }

    public void setIdSystem(String idSystem) {
        this.idSystem = idSystem;
    }

    public List<BusinessEntity> getDeleteEntities() {
        return deleteEntities;
    }

    public void setDeleteEntities(List<BusinessEntity> deleteEntities) {
        this.deleteEntities = deleteEntities;
    }

    public EntityType getDeleteEntityType() {
        return deleteEntityType;
    }

    public void setDeleteEntityType(EntityType deleteEntityType) {
        this.deleteEntityType = deleteEntityType;
    }

    public List<String> getDeleteStreamIds() {
        return deleteStreamIds;
    }

    public void setDeleteStreamIds(List<String> deleteStreamIds) {
        this.deleteStreamIds = deleteStreamIds;
    }

    public SourceFile getSourceFile() {
        return sourceFile;
    }

    public void setSourceFile(SourceFile sourceFile) {
        this.sourceFile = sourceFile;
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
        return "DeleteRequest{" + "user='" + user + '\'' + ", filename='" + filename + '\'' + ", idEntity=" + idEntity
                + ", idSystem='" + idSystem + '\'' + ", deleteEntities=" + deleteEntities + ", deleteStreamIds="
                + deleteStreamIds + ", deleteEntityType=" + deleteEntityType + ", hardDelete=" + hardDelete
                + ", fromDate='" + fromDate + '\'' + ", toDate='" + toDate + '\'' + ", sourceFile=" + sourceFile + '}';
    }
}
