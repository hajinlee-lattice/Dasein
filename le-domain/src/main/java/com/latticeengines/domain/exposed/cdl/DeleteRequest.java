package com.latticeengines.domain.exposed.cdl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.query.BusinessEntity;

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

    @JsonProperty("IdEntity")
    private BusinessEntity idEntity;

    @JsonProperty("DeleteEntities")
    private List<BusinessEntity> deleteEntities;

    @JsonProperty("DeleteStreamIds")
    private List<String> deleteStreamIds;

    @JsonProperty("HardDelete")
    private Boolean hardDelete;

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

    public SourceFile getSourceFile() {
        return sourceFile;
    }

    public void setSourceFile(SourceFile sourceFile) {
        this.sourceFile = sourceFile;
    }
}
