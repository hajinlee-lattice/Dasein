package com.latticeengines.domain.exposed.serviceflows.cdl.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;

public class GenerateRatingStepConfiguration extends MicroserviceStepConfiguration {

    @JsonProperty("dataCollectionVersion")
    private DataCollection.Version dataCollectionVersion;

    @JsonProperty("apsRollupPeriod")
    private String apsRollupPeriod;

    public DataCollection.Version getDataCollectionVersion() {
        return dataCollectionVersion;
    }

    public void setDataCollectionVersion(DataCollection.Version dataCollectionVersion) {
        this.dataCollectionVersion = dataCollectionVersion;
    }

    public String getApsRollupPeriod() {
        return apsRollupPeriod;
    }

    public void setApsRollupPeriod(String apsRollupPeriod) {
        this.apsRollupPeriod = apsRollupPeriod;
    }
}
