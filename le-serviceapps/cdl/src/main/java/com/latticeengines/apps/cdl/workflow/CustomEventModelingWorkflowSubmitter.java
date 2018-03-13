package com.latticeengines.apps.cdl.workflow;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExportType;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.serviceflows.cdl.CustomEventModelingWorkflowConfiguration;

@Component
public class CustomEventModelingWorkflowSubmitter extends WorkflowSubmitter {

    private static final Logger log = LoggerFactory.getLogger(CustomEventModelingWorkflowSubmitter.class);

    public ApplicationId submit() {

        CustomEventModelingWorkflowConfiguration configuration = generateConfiguration();
        return workflowJobService.submit(configuration);
    }

    private CustomEventModelingWorkflowConfiguration generateConfiguration() {
        MetadataSegmentExport metadataSegmentExport = new MetadataSegmentExport();
        metadataSegmentExport.setType(MetadataSegmentExportType.ACCOUNT_ID);
        metadataSegmentExport.setAccountFrontEndRestriction(new FrontEndRestriction());
        return new CustomEventModelingWorkflowConfiguration.Builder() //
                .metadataSegmentExport(metadataSegmentExport).build();
    }
}
