package com.latticeengines.apps.cdl.workflow;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.workflow.annotation.WithWorkflowJobPid;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.domain.exposed.eai.ExportProperty;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.serviceflows.cdl.OrphanRecordExportWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.SegmentExportWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.xpath.operations.Bool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.latticeengines.apps.core.workflow.WorkflowSubmitter;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;

@Component("OrphanRecordExportWorkflowSubmitter")
public class OrphanRecordExportWorkflowSubmitter extends WorkflowSubmitter {
    private static final Logger log = LoggerFactory.getLogger(OrphanRecordExportWorkflowSubmitter.class);

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    @Value("${common.microservice.url}")
    private String microServiceHostPort;

    @Inject
    private DataCollectionProxy dataCollectionProxy;


    @WithWorkflowJobPid
    public ApplicationId submit(String customerSpace, MetadataSegmentExport metadataSegmentExport,
            WorkflowPidWrapper pidWrapper) {

        Map<String, String> inputProperties = new HashMap<>();
        inputProperties.put(WorkflowContextConstants.Inputs.JOB_TYPE, OrphanRecordExportWorkflowConfiguration.WORK_FLOW_NAME);
        String podId = CamilleEnvironment.getPodId();
        String path = metadataSegmentExport.getPath();
        String avroPath = metadataSegmentExport.getPath();

        inputProperties.put(SegmentExportWorkflowConfiguration.SEGMENT_EXPORT_ID, //
                metadataSegmentExport.getExportId());
        inputProperties.put(SegmentExportWorkflowConfiguration.EXPORT_INPUT_PATH, avroPath);
        inputProperties.put(SegmentExportWorkflowConfiguration.EXPORT_OUTPUT_PATH, path);
        inputProperties.put(SegmentExportWorkflowConfiguration.EXPORT_OBJECT_TYPE, //
                "OrphanTransaction");
        inputProperties.put(SegmentExportWorkflowConfiguration.SEGMENT_DISPLAY_NAME, //
                metadataSegmentExport.getExportPrefix());
        inputProperties.put(SegmentExportWorkflowConfiguration.EXPIRE_BY_UTC_TIMESTAMP, //
                new Long(metadataSegmentExport.getCleanupBy().getTime()).toString());
        inputProperties.put(ExportProperty.TARGET_FILE_NAME, getTimestampFromPath(path));

        String txnTableName = getTable(TableRoleInCollection.ConsolidatedDailyTransaction).getName();
        String accountTableName = getTable(TableRoleInCollection.ConsolidatedAccount).getName();
        String productTableName = getTable(TableRoleInCollection.ConsolidatedProduct).getName();

        OrphanRecordExportWorkflowConfiguration configuration = new OrphanRecordExportWorkflowConfiguration.Builder()
                .customer(getCustomerSpace()) //
                .orphanRecordExportId(metadataSegmentExport.getExportId())
                .workflow(OrphanRecordExportWorkflowConfiguration.WORK_FLOW_NAME) //
                .inputProperties(inputProperties) //
                .targetPath(path) //
                .exportInputPath(avroPath) //
                .podId(podId) //
                .tableName(metadataSegmentExport.getTableName()) //
                .internalResourceHostPort(internalResourceHostPort) //
                .microServiceHostPort(microServiceHostPort)
                .txnTableName(txnTableName)
                .accountTableName(accountTableName)
                .productTableName(productTableName)
                .exportMergeFile(Boolean.TRUE)
                .mergedFileName(metadataSegmentExport.getFileName())
                .build();

        configuration.setUserId(metadataSegmentExport.getCreatedBy());
        log.info("end of submit :" + JsonUtils.serialize(configuration));
        return workflowJobService.submit(configuration,pidWrapper.getPid());
    }

    private String getTimestampFromPath(String path) {
        path = path.substring(0, path.length() - 1);
        path = path.substring(path.lastIndexOf("/") + 1, path.length());
        return path;
    }

    private Table getTable(TableRoleInCollection tableRoleInCollection){
        Table txnTable = dataCollectionProxy.getTable(getCustomerSpace().toString(),
                tableRoleInCollection);
        if (txnTable == null){
            throw new RuntimeException("There is no " + tableRoleInCollection + " table.");
        }
        return txnTable;
    }

}
