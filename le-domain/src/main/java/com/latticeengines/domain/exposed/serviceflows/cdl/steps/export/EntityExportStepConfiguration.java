package com.latticeengines.domain.exposed.serviceflows.cdl.steps.export;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;

public class EntityExportStepConfiguration extends BaseStepConfiguration {

    @JsonProperty("customer_space")
    private CustomerSpace customerSpace;

    @JsonProperty("save_to_dropfolder")
    private boolean saveToDropfolder;

    @JsonProperty("data_collection_version")
    private DataCollection.Version dataCollectionVersion;

    @JsonProperty("compress_result")
    private boolean compressResult;

    // for local testing purpose only
    @JsonProperty("save_to_local")
    private boolean saveToLocal;

    @JsonProperty("atlas_export_id")
    private String atlasExportId;

    @JsonProperty("add_export_timestamp")
    private boolean addExportTimestamp;

    public CustomerSpace getCustomerSpace() {
        return customerSpace;
    }

    public void setCustomerSpace(CustomerSpace customerSpace) {
        this.customerSpace = customerSpace;
    }

    public DataCollection.Version getDataCollectionVersion() {
        return dataCollectionVersion;
    }

    public void setDataCollectionVersion(DataCollection.Version dataCollectionVersion) {
        this.dataCollectionVersion = dataCollectionVersion;
    }

    public boolean isSaveToDropfolder() {
        return saveToDropfolder;
    }

    public void setSaveToDropfolder(boolean saveToDropfolder) {
        this.saveToDropfolder = saveToDropfolder;
    }

    public boolean isCompressResult() {
        return compressResult;
    }

    public void setCompressResult(boolean compressResult) {
        this.compressResult = compressResult;
    }

    public boolean isSaveToLocal() {
        return saveToLocal;
    }

    public void setSaveToLocal(boolean saveToLocal) {
        this.saveToLocal = saveToLocal;
    }

    public String getAtlasExportId() {
        return atlasExportId;
    }

    public void setAtlasExportId(String atlasExportId) {
        this.atlasExportId = atlasExportId;
    }

    public boolean isAddExportTimestamp() {
        return addExportTimestamp;
    }

    public void setAddExportTimestamp(boolean addExportTimestamp) {
        this.addExportTimestamp = addExportTimestamp;
    }
}
