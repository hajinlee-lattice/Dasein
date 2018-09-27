package com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;

public class UseConfiguredModelingAttributesConfiguration extends MicroserviceStepConfiguration {

    private DataCollection.Version dataCollectionVersion;

    private boolean excludeDataCloudAttributes;

    private boolean excludeCDLAttributes;

    private Integer modelIteration;

    private RatingEngineType ratingEngineType;

    public DataCollection.Version getDataCollectionVersion() {
        return dataCollectionVersion;
    }

    public void setDataCollectionVersion(DataCollection.Version dataCollectionVersion) {
        this.dataCollectionVersion = dataCollectionVersion;
    }

    public boolean isExcludeDataCloudAttributes() {
        return excludeDataCloudAttributes;
    }

    public void setExcludeDataCloudAttributes(boolean excludeDataCloudAttributes) {
        this.excludeDataCloudAttributes = excludeDataCloudAttributes;
    }

    public boolean isExcludeCDLAttributes() {
        return excludeCDLAttributes;
    }

    public void setExcludeCDLAttributes(boolean excludeCDLAttributes) {
        this.excludeCDLAttributes = excludeCDLAttributes;
    }

    public Integer getModelIteration() {
        return modelIteration;
    }

    public void setModelIteration(Integer modelIteration) {
        this.modelIteration = modelIteration;
    }

    public RatingEngineType getRatingEngineType() {
        return ratingEngineType;
    }

    public void setRatingEngineType(RatingEngineType ratingEngineType) {
        this.ratingEngineType = ratingEngineType;
    }

    public static class Builder {

        private CustomerSpace customerSpace;

        private DataCollection.Version dataCollectionVersion;

        private boolean excludeDataCloudAttributes;

        private boolean excludeCDLAttributes;

        private Integer modelIteration;

        private boolean skipStep = true;

        private RatingEngineType ratingEngineType;

        public UseConfiguredModelingAttributesConfiguration build() {
            UseConfiguredModelingAttributesConfiguration configuration = new UseConfiguredModelingAttributesConfiguration();
            configuration.setCustomerSpace(customerSpace);
            configuration.setDataCollectionVersion(dataCollectionVersion);
            configuration.setExcludeDataCloudAttributes(excludeDataCloudAttributes);
            configuration.setExcludeCDLAttributes(excludeCDLAttributes);
            configuration.setModelIteration(modelIteration);
            configuration.setSkipStep(skipStep);
            configuration.setRatingEngineType(ratingEngineType);
            return configuration;
        }

        public Builder customerSpace(CustomerSpace customerSpace) {
            this.customerSpace = customerSpace;
            return this;
        }

        public Builder dataCollectionVersion(DataCollection.Version dataCollectionVersion) {
            this.dataCollectionVersion = dataCollectionVersion;
            return this;
        }

        public Builder excludeDataCloudAttributes(boolean excludeDataCloudAttributes) {
            this.excludeDataCloudAttributes = excludeDataCloudAttributes;
            return this;
        }

        public Builder excludeCDLAttributes(boolean excludeCDLAttributes) {
            this.excludeCDLAttributes = excludeCDLAttributes;
            return this;
        }

        public Builder modelIteration(Integer modelIteration) {
            this.modelIteration = modelIteration;
            return this;
        }

        public Builder skipStep(boolean skipStep) {
            this.skipStep = skipStep;
            return this;
        }

        public Builder ratingEngineType(RatingEngineType ratingEngineType) {
            this.ratingEngineType = ratingEngineType;
            return this;
        }
    }
}
