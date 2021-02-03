package com.latticeengines.domain.exposed.serviceflows.cdl;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata.CopyListSegmentCSVConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata.ExportListSegmentCSVToS3Configuration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata.ExtractListSegmentCSVConfiguration;

public class ImportListSegmentWorkflowConfiguration extends BaseCDLWorkflowConfiguration {

    public static final String IMPORT_DATA_UNIT_NAME = "IMPORT_DATA_UNIT_NAME";
    public static final String ACCOUNT_DATA_UNIT_NAME = "ACCOUNT_DATA_UNIT_NAME";
    public static final String CONTACT_DATA_UNIT_NAME = "CONTACT_DATA_UNIT_NAME";
    public static final String PREVIOUS_ACCOUNT_ATHENA_UNIT_NAME = "PREVIOUS_ATHENA_UNIT_NAME";

    public ImportListSegmentWorkflowConfiguration() {
    }

    public static class Builder {
        private ImportListSegmentWorkflowConfiguration configuration = new ImportListSegmentWorkflowConfiguration();
        private CopyListSegmentCSVConfiguration copyListSegmentCSVConfiguration = new CopyListSegmentCSVConfiguration();
        private ExtractListSegmentCSVConfiguration extractListSegmentCSVConfiguration = new ExtractListSegmentCSVConfiguration();
        private ExportListSegmentCSVToS3Configuration exportListSegmentCSVToS3Configuration = new ExportListSegmentCSVToS3Configuration();

        public Builder customer(CustomerSpace customerSpace) {
            configuration.setContainerConfiguration("importListSegmentWorkflow", customerSpace, configuration.getClass().getSimpleName());
            copyListSegmentCSVConfiguration.setCustomerSpace(customerSpace);
            extractListSegmentCSVConfiguration.setCustomerSpace(customerSpace);
            exportListSegmentCSVToS3Configuration.setCustomerSpace(customerSpace);
            return this;
        }

        public Builder sourceKey(String sourceKey) {
            copyListSegmentCSVConfiguration.setSourceKey(sourceKey);
            return this;
        }

        public Builder sourceBucket(String sourceBucket) {
            copyListSegmentCSVConfiguration.setSourceBucket(sourceBucket);
            return this;
        }

        public Builder destBucket(String destBucket) {
            copyListSegmentCSVConfiguration.setDestBucket(destBucket);
            return this;
        }

        public Builder segmentName(String segmentName) {
            copyListSegmentCSVConfiguration.setSegmentName(segmentName);
            extractListSegmentCSVConfiguration.setSegmentName(segmentName);
            return this;
        }

        public Builder inputProperties(Map<String, String> inputProperties) {
            configuration.setInputProperties(inputProperties);
            return this;
        }

        public Builder isSSVITenant(boolean isSSVITenant) {
            extractListSegmentCSVConfiguration.setSSVITenant(isSSVITenant);
            return this;
        }

        public Builder isCDLTenant(boolean isCDLTenant) {
            extractListSegmentCSVConfiguration.setCDLTenant(isCDLTenant);
            return this;
        }

        public Builder systemIdMaps(Map<String, List<String>> systemIdMaps) {
            extractListSegmentCSVConfiguration.setSystemIdMaps(systemIdMaps);
            return this;
        }

        public ImportListSegmentWorkflowConfiguration build() {
            configuration.add(copyListSegmentCSVConfiguration);
            configuration.add(extractListSegmentCSVConfiguration);
            configuration.add(exportListSegmentCSVToS3Configuration);
            return configuration;
        }
    }
}
