package com.latticeengines.cdl.workflow.steps.process;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.cdl.workflow.steps.export.ExportDataToRedshift;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedImport;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessStepConfiguration;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("startProcessing")
public class StartProcessing extends BaseWorkflowStep<ProcessStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(StartProcessing.class);

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private DataFeedProxy dataFeedProxy;

    @Inject
    private WorkflowProxy workflowProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private ExportDataToRedshift exportDataToRedshift;

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    private CustomerSpace customerSpace;
    private DataCollection.Version activeVersion;
    private DataCollection.Version inactiveVersion;
    private InternalResourceRestApiProxy internalResourceProxy;

    @PostConstruct
    public void init() {
        internalResourceProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
    }

    @Override
    public void execute() {
        determineVersions();

        DataFeedExecution execution = dataFeedProxy
                .updateExecutionWorkflowId(configuration.getCustomerSpace().toString(), jobId);
        log.info(String.format("current running execution %s", execution));

        DataFeed datafeed = dataFeedProxy.getDataFeed(configuration.getCustomerSpace().toString());
        execution = datafeed.getActiveExecution();

        if (execution == null) {
            putObjectInContext(CONSOLIDATE_INPUT_IMPORTS, Collections.emptyMap());
        } else if (execution.getWorkflowId().longValue() != jobId.longValue()) {
            throw new RuntimeException(
                    String.format("current active execution has a workflow id %s, which is different from %s ",
                            execution.getWorkflowId(), jobId));
        } else {
            setEntityImportsMap(execution);
        }

        List<Job> importJobs = getJobs(configuration.getImportJobIds());
        createReport(importJobs);
        updateActions();
        cleanupInactiveVersion();
        exportDataToRedshift.upsertToInactiveVersion();
    }

    private void determineVersions() {
        customerSpace = configuration.getCustomerSpace();
        activeVersion = dataCollectionProxy.getActiveVersion(customerSpace.toString());
        inactiveVersion = activeVersion.complement();
        putObjectInContext(CDL_ACTIVE_VERSION, activeVersion);
        putObjectInContext(CDL_INACTIVE_VERSION, inactiveVersion);
        putObjectInContext(CUSTOMER_SPACE, customerSpace.toString());
        log.info(String.format("Active version is %s, inactive version is %s", //
                activeVersion.name(), inactiveVersion.name()));
    }

    private void setEntityImportsMap(DataFeedExecution execution) {
        Map<BusinessEntity, List<DataFeedImport>> entityImportsMap = new HashMap<>();
        execution.getImports().forEach(i -> {
            BusinessEntity entity = BusinessEntity.valueOf(i.getEntity());
            entityImportsMap.putIfAbsent(entity, new ArrayList<>());
            entityImportsMap.get(entity).add(i);
        });
        putObjectInContext(CONSOLIDATE_INPUT_IMPORTS, entityImportsMap);
    }

    private List<Job> getJobs(List<Long> importJobIds) {
        if (importJobIds.isEmpty()) {
            return Collections.emptyList();
        }
        return workflowProxy.getWorkflowExecutionsByJobIds(
                importJobIds.stream().map(jobId -> jobId.toString()).collect(Collectors.toList()));
    }

    private void updateActions() {
        List<Long> actionIds = configuration.getActionIds();
        log.info(String.format("Updating actions=%s", Arrays.toString(actionIds.toArray())));
        if (CollectionUtils.isNotEmpty(actionIds)) {
            internalResourceProxy.updateOwnerIdIn(configuration.getCustomerSpace().toString(), jobId, actionIds);
        }
    }

    private void createReport(List<Job> jobs) {
        ObjectNode json = JsonUtils.createObjectNode();
        ArrayNode arrayNode = json.putArray(ReportPurpose.IMPORT_SUMMARY.getKey());
        jobs.forEach(job -> {
            String fileName = job.getInputs().get(WorkflowContextConstants.Inputs.SOURCE_DISPLAY_NAME);
            Report importReport = job.getReports().stream() //
                    .filter(r -> r.getPurpose().equals(ReportPurpose.IMPORT_SUMMARY))//
                    .findFirst().orElse(null);
            if (importReport != null) {
                ObjectNode importReportNode = JsonUtils.createObjectNode();
                try {
                    importReportNode.set(fileName,
                            JsonUtils.getObjectMapper().readTree(importReport.getJson().getPayload()));
                    arrayNode.add(importReportNode);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } else {
                log.info(String.format("import job %s has no report generated.", fileName));
            }
        });
        Report report = createReport(json.toString(), ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY,
                UUID.randomUUID().toString());
        registerReport(configuration.getCustomerSpace(), report);
    }

    private void cleanupInactiveVersion() {
        for (TableRoleInCollection role : TableRoleInCollection.values()) {
            String tableName = dataCollectionProxy.getTableName(customerSpace.toString(), role, inactiveVersion);
            if (StringUtils.isNotBlank(tableName)) {
                log.info("Removing table " + tableName + " as " + role + " in " + inactiveVersion);
                metadataProxy.deleteTable(customerSpace.toString(), tableName);
            }
        }
    }

}
