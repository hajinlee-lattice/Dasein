package com.latticeengines.cdl.workflow.steps;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedImport;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.ConsolidateAndPublishWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.ConsolidateDataBaseConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.StartExecutionConfiguration;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.domain.exposed.workflow.BaseWrapperStepConfiguration.Phase;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("startExecution")
public class StartExecution extends BaseWorkflowStep<StartExecutionConfiguration> {

    @Autowired
    private DataFeedProxy dataFeedProxy;

    @Autowired
    private WorkflowProxy workflowProxy;

    @Override
    public void execute() {
        DataFeedExecution execution = dataFeedProxy
                .updateExecutionWorkflowId(configuration.getCustomerSpace().toString(), jobId);
        log.info(String.format("current running execution %s", execution));

        DataFeed datafeed = dataFeedProxy.getDataFeed(configuration.getCustomerSpace().toString());
        execution = datafeed.getActiveExecution();

        if (execution == null) {
            putObjectInContext(CONSOLIDATE_INPUT_IMPORTS, Collections.EMPTY_MAP);
        } else if (execution.getWorkflowId().longValue() != jobId.longValue()) {
            throw new RuntimeException(
                    String.format("current active execution has a workflow id %s, which is different from %s ",
                            execution.getWorkflowId(), jobId));
        } else {
            setConsolidateInputImports(execution);
        }

        List<Job> importJobs = getJobs(configuration.getImportJobIds());
        createReport(importJobs);
        // updating import job logic starts here
    }

    private void setConsolidateInputImports(DataFeedExecution execution) {
        Map<String, BaseStepConfiguration> stepConfigMap = getStepConfigMapInWorkflow(
                ConsolidateAndPublishWorkflowConfiguration.class);
        if (stepConfigMap.isEmpty()) {
            log.info("stepConfigMap is Empty!!!");
        }
        Map<BusinessEntity, List<DataFeedImport>> entityImportsMap = new HashMap<>();
        execution.getImports().forEach(i -> {
            BusinessEntity entity = BusinessEntity.valueOf(i.getEntity());
            entityImportsMap.putIfAbsent(entity, new ArrayList<>());
            entityImportsMap.get(entity).add(i);
            stepConfigMap.entrySet().stream()
                    .filter(e -> (e.getValue() instanceof ConsolidateDataBaseConfiguration
                            && ((ConsolidateDataBaseConfiguration) e.getValue()).getBusinessEntity().equals(entity)))
                    .forEach(e -> {
                        log.info("enabling consolidate step:" + e.getKey());
                        e.getValue().setSkipStep(false);
                        ((ConsolidateDataBaseConfiguration) e.getValue()).setPhase(Phase.PRE_PROCESSING);
                        putObjectInContext(e.getKey(), e.getValue());
                    });
        });
        putObjectInContext(CONSOLIDATE_INPUT_IMPORTS, entityImportsMap);
    }

    private List<Job> getJobs(List<Long> importJobIds) {
        if (importJobIds.isEmpty()) {
            return Collections.emptyList();
        }
        return importJobIds.stream().map(id -> workflowProxy.getWorkflowExecution(String.valueOf(id)))
                .collect(Collectors.toList());
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

}