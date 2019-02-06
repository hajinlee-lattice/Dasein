package com.latticeengines.pls.workflow;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.domain.exposed.eai.ExportProperty;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.serviceflows.cdl.SegmentExportWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;

@Component("segmentExportWorkflowSubmitter")
public class SegmentExportWorkflowSubmitter extends WorkflowSubmitter {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(SegmentExportWorkflowSubmitter.class);

    public ApplicationId submit(MetadataSegmentExport metadataSegmentExport) {
        Map<String, String> inputProperties = new HashMap<>();
        inputProperties.put(WorkflowContextConstants.Inputs.JOB_TYPE, "segmentExportWorkflow");
        String podId = CamilleEnvironment.getPodId();

        String path = metadataSegmentExport.getPath();
        String avroPath = metadataSegmentExport.getPath();

        inputProperties.put(SegmentExportWorkflowConfiguration.SEGMENT_EXPORT_ID, //
                metadataSegmentExport.getExportId());
        inputProperties.put(SegmentExportWorkflowConfiguration.EXPORT_INPUT_PATH, avroPath);
        inputProperties.put(SegmentExportWorkflowConfiguration.EXPORT_OUTPUT_PATH, path);
        inputProperties.put(SegmentExportWorkflowConfiguration.EXPORT_OBJECT_TYPE, //
                metadataSegmentExport.getType().getDisplayName());
        inputProperties.put(SegmentExportWorkflowConfiguration.SEGMENT_DISPLAY_NAME, //
                metadataSegmentExport.getExportPrefix());
        inputProperties.put(SegmentExportWorkflowConfiguration.EXPIRE_BY_UTC_TIMESTAMP, //
                Long.toString(metadataSegmentExport.getCleanupBy().getTime()));
        inputProperties.put(ExportProperty.TARGET_FILE_NAME, getTimestampFromPath(path));

        SegmentExportWorkflowConfiguration configuration = new SegmentExportWorkflowConfiguration.Builder()
                .customer(getCustomerSpace()) //
                .workflow("segmentExportWorkflow") //
                .inputProperties(inputProperties) //
                .metadataSegmentExportId(metadataSegmentExport.getExportId()) //
                .targetPath(path) //
                .exportInputPath(avroPath) //
                .podId(podId) //
                .tableName(metadataSegmentExport.getTableName()) //
                .internalResourceHostPort(internalResourceHostPort) //
                .microServiceHostPort(microserviceHostPort) //
                .build();
        return workflowJobService.submit(configuration);
    }

    private String getTimestampFromPath(String path) {
        path = path.substring(0, path.length() - 1);
        path = path.substring(path.lastIndexOf("/") + 1);
        return path;
    }
}
