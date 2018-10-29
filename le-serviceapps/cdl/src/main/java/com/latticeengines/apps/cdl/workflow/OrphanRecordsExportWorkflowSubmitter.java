package com.latticeengines.apps.cdl.workflow;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.OrphanRecordsType;
import com.latticeengines.domain.exposed.cdl.OrphanRecordsExportRequest;
import com.latticeengines.domain.exposed.eai.ExportProperty;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.serviceflows.cdl.OrphanRecordsExportWorkflowConfiguration;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.workflow.annotation.WithWorkflowJobPid;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;

@Component("orphanRecordsExportWorkflowSubmitter")
public class OrphanRecordsExportWorkflowSubmitter extends WorkflowSubmitter {
    private static final Logger log = LoggerFactory.getLogger(OrphanRecordsExportWorkflowSubmitter.class);

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    @Value("${common.microservice.url}")
    private String microServiceHostPort;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @WithWorkflowJobPid
    public ApplicationId submit(String customerSpace, OrphanRecordsExportRequest request,
                                WorkflowPidWrapper pidWrapper) {
        String podId = CamilleEnvironment.getPodId();
        OrphanRecordsType orphanRecordsType = request.getOrphanRecordsType();

        if (StringUtils.isBlank(request.getExportId())) {
            request.setExportId(UUID.randomUUID().toString());
        }

        if (StringUtils.isBlank(request.getDataCollectionName())) {
            String name = dataCollectionProxy.getDefaultDataCollection(customerSpace).getName();
            request.setDataCollectionName(name);
        }

        if (request.getArtifactVersion() == null) {
            request.setArtifactVersion(dataCollectionProxy.getActiveVersion(customerSpace));
        }
        log.info("Use artifact version=" + request.getArtifactVersion().name());

        String targetPath = PathBuilder.buildDataFileExportPath(podId, CustomerSpace.parse(customerSpace)).toString();
        log.info("Use targetPath=" + targetPath);

        Map<String, String> inputProperties = new HashMap<>();
        inputProperties.put(WorkflowContextConstants.Inputs.JOB_TYPE,
                OrphanRecordsExportWorkflowConfiguration.WORKFLOW_NAME);
        inputProperties.put(OrphanRecordsExportWorkflowConfiguration.CREATED_BY, request.getCreatedBy());
        inputProperties.put(OrphanRecordsExportWorkflowConfiguration.EXPORT_ID, request.getExportId());
        inputProperties.put(OrphanRecordsExportWorkflowConfiguration.ARTIFACT_TYPE,
                request.getOrphanRecordsType().name());
        inputProperties.put(ExportProperty.TARGET_FILE_NAME, request.getOrphanRecordsType().getOrphanType());
        log.info("InputProperties=" + JsonUtils.serialize(inputProperties));

        String transactionTableName = getTable(TableRoleInCollection.ConsolidatedDailyTransaction,
                request.getArtifactVersion()).getName();
        String accountTableName = getTable(TableRoleInCollection.ConsolidatedAccount,
                request.getArtifactVersion()).getName();
        String contactTableName = getTable(TableRoleInCollection.ConsolidatedContact,
                request.getArtifactVersion()).getName();
        String productTableName = getTable(TableRoleInCollection.ConsolidatedProduct,
                request.getArtifactVersion()).getName();

        OrphanRecordsExportWorkflowConfiguration wfConfig = new OrphanRecordsExportWorkflowConfiguration.Builder()
                .customer(getCustomerSpace()) //
                .workflow(OrphanRecordsExportWorkflowConfiguration.WORKFLOW_NAME) //
                .orphanRecordExportId(request.getExportId()) //
                .orphanRecordsType(orphanRecordsType) //
                .inputProperties(inputProperties) //
                .targetPath(targetPath) //
                .exportInputPath(targetPath) //
                .podId(podId) //
                .targetTableName(NamingUtils.timestamp("OrphanRecordsTable")) //
                .internalResourceHostPort(internalResourceHostPort) //
                .microServiceHostPort(microServiceHostPort) //
                .dataCollectionName(request.getDataCollectionName()) //
                .dataCollectionVersion(request.getArtifactVersion()) //
                .accountTableName(accountTableName) //
                .contactTableName(contactTableName) //
                .productTableName(productTableName) //
                .transactionTableName(transactionTableName) //
                .exportMergeFile(Boolean.TRUE) //
                .mergedFileName(orphanRecordsType.getOrphanType() + ".csv") //
                .build();
        wfConfig.setUserId(request.getCreatedBy());
        log.info("Workflow config=" + JsonUtils.serialize(request));

        return workflowJobService.submit(wfConfig, pidWrapper.getPid());
    }

//    private String getTimestampFromPath(String path) {
//        path = path.substring(0, path.length() - 1);
//        path = path.substring(path.lastIndexOf("/") + 1);
//        return path;
//    }

    private Table getTable(TableRoleInCollection tableRoleInCollection, DataCollection.Version version) {
        Table table = dataCollectionProxy.getTable(getCustomerSpace().toString(), tableRoleInCollection, version);
        if (table == null) {
            throw new RuntimeException(
                    String.format("Cannot get table %s from version %s.", tableRoleInCollection, version));
        }
        return table;
    }
}
