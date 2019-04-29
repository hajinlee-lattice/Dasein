package com.latticeengines.domain.exposed.serviceflows.cdl.steps.export;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ExportEntity;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;

public class EntityExportStepConfiguration extends BaseStepConfiguration {

    @JsonProperty("customer_space")
    private CustomerSpace customerSpace;

    @JsonProperty("save_to_dropfolder")
    private boolean saveToDropfolder;

    @JsonProperty("data_collection_version")
    private DataCollection.Version dataCollectionVersion;

    // if null export all records of the requested entity
    @JsonProperty("front_end_query")
    private FrontEndQuery frontEndQuery;

    @JsonProperty("export_entities")
    private List<ExportEntity> exportEntities;

    @JsonProperty("compress_result")
    private boolean compressResult;

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

    public FrontEndQuery getFrontEndQuery() {
        return frontEndQuery;
    }

    public void setFrontEndQuery(FrontEndQuery frontEndQuery) {
        this.frontEndQuery = frontEndQuery;
    }

    public List<ExportEntity> getExportEntities() {
        return exportEntities;
    }

    public void setExportEntities(List<ExportEntity> exportEntities) {
        this.exportEntities = exportEntities;
    }

    public boolean isCompressResult() {
        return compressResult;
    }

    public void setCompressResult(boolean compressResult) {
        this.compressResult = compressResult;
    }
}
