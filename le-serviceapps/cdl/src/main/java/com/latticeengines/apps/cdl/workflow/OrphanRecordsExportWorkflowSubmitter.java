package com.latticeengines.apps.cdl.workflow;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.ProxyResourceService;
import com.latticeengines.apps.core.service.AttrConfigService;
import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.workflow.annotation.WithWorkflowJobPid;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.OrphanRecordsExportRequest;
import com.latticeengines.domain.exposed.cdl.OrphanRecordsType;
import com.latticeengines.domain.exposed.eai.ExportProperty;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionArtifact;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;
import com.latticeengines.domain.exposed.serviceflows.cdl.OrphanRecordsExportWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;

@Component("orphanRecordsExportWorkflowSubmitter")
public class OrphanRecordsExportWorkflowSubmitter extends WorkflowSubmitter {
    private static final Logger log = LoggerFactory.getLogger(OrphanRecordsExportWorkflowSubmitter.class);

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    @Value("${common.microservice.url}")
    private String microServiceHostPort;

    @Inject
    private ProxyResourceService proxyResourceService;

    @Inject
    private AttrConfigService attrConfigService;

    @WithWorkflowJobPid
    public ApplicationId submit(String customerSpace, OrphanRecordsExportRequest request,
                                WorkflowPidWrapper pidWrapper) {
        String podId = CamilleEnvironment.getPodId();
        OrphanRecordsType orphanRecordsType = request.getOrphanRecordsType();

        if (StringUtils.isBlank(request.getExportId())) {
            request.setExportId(UUID.randomUUID().toString());
        }
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        if (StringUtils.isBlank(request.getDataCollectionName())) {
            String name = proxyResourceService.getDataCollection(customerSpace).getName();
            request.setDataCollectionName(name);
        }
        if (request.getArtifactVersion() == null) {
            request.setArtifactVersion(proxyResourceService.getDataCollection(customerSpace).getVersion());
        }
        log.info("Use artifact version=" + request.getArtifactVersion().name());

        String targetPathSuffix = NamingUtils.timestamp(orphanRecordsType.getOrphanType());
        String targetPath = PathBuilder
                .buildDataFileExportPath(podId, CustomerSpace.parse(customerSpace))
                .append(targetPathSuffix).toString();
        log.info("Use targetPath=" + targetPath);

        DataCollectionArtifact artifact = new DataCollectionArtifact();
        artifact.setName(orphanRecordsType.getOrphanType());
        artifact.setUrl(null);
        artifact.setStatus(request.getOrphanRecordsArtifactStatus());
        artifact = proxyResourceService.createArtifact(customerSpace, artifact,request.getArtifactVersion());
        log.info("Created dataCollectionArtifact=" + JsonUtils.serialize(artifact));

        List<Attribute> importedAttributes = getImportAttributes(orphanRecordsType.getDataSource(),
                orphanRecordsType.getDataFeedType(), orphanRecordsType.getEntity().name());

        Map<String, String> inputProperties = new HashMap<>();
        inputProperties.put(WorkflowContextConstants.Inputs.JOB_TYPE,
                OrphanRecordsExportWorkflowConfiguration.WORKFLOW_NAME);
        inputProperties.put(OrphanRecordsExportWorkflowConfiguration.CREATED_BY, request.getCreatedBy());
        inputProperties.put(OrphanRecordsExportWorkflowConfiguration.EXPORT_ID, request.getExportId());
        inputProperties.put(OrphanRecordsExportWorkflowConfiguration.ARTIFACT_TYPE, orphanRecordsType.name());
        inputProperties.put(OrphanRecordsExportWorkflowConfiguration.ARTIFACT_DISPLAY_NAME,
                orphanRecordsType.getDisplayName());
        inputProperties.put(ExportProperty.TARGET_FILE_NAME, targetPathSuffix);
        log.info("InputProperties=" + JsonUtils.serialize(inputProperties));

        String transactionTableName = getTableName(TableRoleInCollection.ConsolidatedRawTransaction,
                request.getArtifactVersion());
        String accountTableName = getTableName(TableRoleInCollection.ConsolidatedAccount,
                request.getArtifactVersion());
        String contactTableName = getTableName(TableRoleInCollection.ConsolidatedContact,
                request.getArtifactVersion());
        String productTableName = getTableName(TableRoleInCollection.ConsolidatedProduct,
                request.getArtifactVersion());

        AttrConfigRequest attrRequest = new AttrConfigRequest();
        List<AttrConfig> attrConfigs = attrConfigService.getRenderedList(ColumnSelection.Predefined.Enrichment.getName(), true);
        attrRequest.setAttrConfigs(attrConfigs);
        if (CollectionUtils.isEmpty(attrRequest.getAttrConfigs())) {
            return null;
        }
        List<String> validatedColumns = attrRequest.getAttrConfigs().stream().map(config -> config.getAttrName())
                .collect(Collectors.toList());

        if (orphanRecordsType == OrphanRecordsType.TRANSACTION &&
                !validateTableNames(new String[] { transactionTableName, accountTableName, productTableName })) {
            return null;
        }

        if (orphanRecordsType == OrphanRecordsType.CONTACT &&
                !validateTableNames(new String[] { contactTableName, accountTableName })) {
            return null;
        }

        if (orphanRecordsType == OrphanRecordsType.UNMATCHED_ACCOUNT &&
                !validateTableNames(new String[] { accountTableName })) {
            return null;
        }

        OrphanRecordsExportWorkflowConfiguration wfConfig = new OrphanRecordsExportWorkflowConfiguration.Builder()
                .customer(getCustomerSpace()) //
                .workflow(OrphanRecordsExportWorkflowConfiguration.WORKFLOW_NAME) //
                .orphanRecordExportId(request.getExportId()) //
                .orphanRecordsType(orphanRecordsType) //
                .originalAttributeNames(importedAttributes) //
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
                .validatedColumns(validatedColumns) //
                .mergedFileName(orphanRecordsType.getOrphanType() + ".csv") //
                .build();
        wfConfig.setUserId(request.getCreatedBy());
        log.info("Workflow config=" + JsonUtils.serialize(request));

        return workflowJobService.submit(wfConfig, pidWrapper.getPid());
    }

    private String getTableName(TableRoleInCollection tableRoleInCollection, DataCollection.Version version) {
        Table table = proxyResourceService.getTable(getCustomerSpace().toString(), tableRoleInCollection,
                version);
        if (table == null) {
            return null;
        }
        return table.getName();
    }

    private List<Attribute> getImportAttributes(String source, String dataFeedType, String entity) {
        String tenant = getCustomerSpace().toString();
        DataFeedTask task = proxyResourceService.getDataFeedTask(tenant, source, dataFeedType, entity);

        if (task == null) {
            return null;
        }

        return task.getImportTemplate().getAttributes();
    }

    private boolean validateTableNames(String[] tableNames) {
        for (String tableName : tableNames) {
            if (StringUtils.isBlank(tableName)) {
                return false;
            }
        }
        return true;
    }
}
