package com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;

public class SegmentExportStepConfiguration extends BaseStepConfiguration {
    private CustomerSpace customerSpace;

    private String metadataSegmentExportId;

    private MetadataSegmentExport metadataSegmentExport;

    public CustomerSpace getCustomerSpace() {
        return customerSpace;
    }

    public void setCustomerSpace(CustomerSpace customerSpace) {
        this.customerSpace = customerSpace;
    }

    public String getMetadataSegmentExportId() {
        return metadataSegmentExportId;
    }

    public void setMetadataSegmentExportId(String metadataSegmentExportId) {
        this.metadataSegmentExportId = metadataSegmentExportId;
    }

    public MetadataSegmentExport getMetadataSegmentExport() {
        return metadataSegmentExport;
    }

    public void setMetadataSegmentExport(MetadataSegmentExport metadataSegmentExport) {
        this.metadataSegmentExport = metadataSegmentExport;
    }
}
