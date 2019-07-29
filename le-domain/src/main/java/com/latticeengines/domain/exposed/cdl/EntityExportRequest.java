package com.latticeengines.domain.exposed.cdl;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.DataCollection;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class EntityExportRequest {

    @JsonProperty("DataCollectionVersion")
    private DataCollection.Version dataCollectionVersion;

    public DataCollection.Version getDataCollectionVersion() {
        return dataCollectionVersion;
    }

    public void setDataCollectionVersion(DataCollection.Version dataCollectionVersion) {
        this.dataCollectionVersion = dataCollectionVersion;
    }

    @JsonProperty("AtlasExportId")
    private String atlasExportId;

    public String getAtlasExportId() {
        return atlasExportId;
    }

    public void setAtlasExportId(String atlasExportId) {
        this.atlasExportId = atlasExportId;
    }
}
