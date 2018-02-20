package com.latticeengines.cdl.workflow.steps.process;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.cdl.workflow.steps.export.ExportDataToRedshift;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedImport;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.ProcessAnalyzeWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.BaseProcessEntityStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessStepConfiguration;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.domain.exposed.workflow.BaseWrapperStepConfiguration;
import com.latticeengines.domain.exposed.workflow.BaseWrapperStepConfiguration.Phase;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("startProcessing")
public class StartProcessing extends BaseWorkflowStep<ProcessStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(StartProcessing.class);

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private DataFeedProxy dataFeedProxy;

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
    private ObjectNode reportJson;

    @PostConstruct
    public void init() {
        internalResourceProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
    }

    // for Spring
    public StartProcessing() {
    }

    @VisibleForTesting
    StartProcessing(DataCollectionProxy dataCollectionProxy, InternalResourceRestApiProxy internalResourceProxy,
            CustomerSpace customerSpace) {
        this.dataCollectionProxy = dataCollectionProxy;
        this.internalResourceProxy = internalResourceProxy;
        this.customerSpace = customerSpace;
        this.reportJson = JsonUtils.createObjectNode();
    }

    @Override
    public void execute() {
        customerSpace = configuration.getCustomerSpace();
        determineVersions();

        DataFeedExecution execution = dataFeedProxy.updateExecutionWorkflowId(customerSpace.toString(), jobId);
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

        createReportJson();
        updateActions();
        setRebuildEntities();
        setupInactiveVersion();
        exportDataToRedshift.upsertToInactiveVersion();
        // clearPhaseForRetry();
    }

    @SuppressWarnings("unused")
    private void clearPhaseForRetry() {
        Map<String, BaseStepConfiguration> stepConfigMap = getStepConfigMapInWorkflow(
                ProcessAnalyzeWorkflowConfiguration.class);
        if (stepConfigMap.isEmpty()) {
            log.info("stepConfigMap is Empty!!!");
        }
        stepConfigMap.entrySet().stream().filter(e -> e.getValue() instanceof BaseWrapperStepConfiguration)//
                .forEach(e -> {
                    log.info("enabling step:" + e.getKey());
                    e.getValue().setSkipStep(false);
                    ((BaseWrapperStepConfiguration) e.getValue()).setPhase(Phase.PRE_PROCESSING);
                    putObjectInContext(e.getKey(), e.getValue());
                });
    }

    private void setRebuildEntities() {
        Set<BusinessEntity> rebuildEntities = RebuildEntitiesProvider.getRebuildEntities(this);
        Map<String, BaseStepConfiguration> stepConfigMap = getStepConfigMapInWorkflow(
                ProcessAnalyzeWorkflowConfiguration.class);
        if (stepConfigMap.isEmpty()) {
            log.info("stepConfigMap is Empty!!!");
        }

        stepConfigMap.entrySet().stream().filter(e -> (e.getValue() instanceof BaseProcessEntityStepConfiguration
                && (rebuildEntities.contains(((BaseProcessEntityStepConfiguration) e.getValue()).getMainEntity()))))
                .forEach(e -> {
                    log.info("enabled rebuidling step of:" + e.getKey());
                    ((BaseProcessEntityStepConfiguration) e.getValue()).setRebuild(Boolean.TRUE);
                    putObjectInContext(e.getKey(), e.getValue());
                });
    }

    private List<Job> getDeleteJobs() {
        return internalResourceProxy.findJobsBasedOnActionIdsAndType(customerSpace.toString(),
                configuration.getActionIds(), ActionType.CDL_OPERATION_WORKFLOW);
    }

    private void determineVersions() {
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

    private void updateActions() {
        List<Long> actionIds = configuration.getActionIds();
        log.info(String.format("Updating actions=%s", Arrays.toString(actionIds.toArray())));
        if (CollectionUtils.isNotEmpty(actionIds)) {
            internalResourceProxy.updateOwnerIdIn(configuration.getCustomerSpace().toString(), jobId, actionIds);
        }
    }

    private void createReportJson() {
        reportJson = JsonUtils.createObjectNode();
        putObjectInContext(ReportPurpose.PROCESS_ANALYZE_RECORDS_SUMMARY.getKey(), reportJson);
    }

    private void setupInactiveVersion() {
        for (TableRoleInCollection role : TableRoleInCollection.values()) {
            String tableName = dataCollectionProxy.getTableName(customerSpace.toString(), role, inactiveVersion);
            if (StringUtils.isNotBlank(tableName)) {
                String activeTableName = dataCollectionProxy.getTableName(customerSpace.toString(), role,
                        activeVersion);
                if (tableName.equalsIgnoreCase(activeTableName)) {
                    log.info("Unlink table " + tableName + " as " + role + " in " + inactiveVersion);
                    dataCollectionProxy.unlinkTable(customerSpace.toString(), tableName, role, inactiveVersion);
                } else {
                    log.info("Removing table " + tableName + " as " + role + " in " + inactiveVersion);
                    metadataProxy.deleteTable(customerSpace.toString(), tableName);
                }
            }
        }
        log.info("Removing stats in " + inactiveVersion);
        dataCollectionProxy.removeStats(customerSpace.toString(), inactiveVersion);
    }

    public static class RebuildEntitiesProvider {
        public static Set<BusinessEntity> getRebuildEntities(StartProcessing st) {
            Set<BusinessEntity> rebuildEntities = new HashSet<>();
            Collection<Class<? extends RebuildEntitiesTemplate>> decrators = Arrays
                    .asList(RebuildOnDLVersionTemplate.class, RebuildOnDeleteJobTemplate.class);
            for (Class<? extends RebuildEntitiesTemplate> c : decrators) {
                try {
                    RebuildEntitiesTemplate template = ((RebuildEntitiesTemplate) c
                            .getDeclaredConstructor(StartProcessing.class).newInstance(st));
                    rebuildEntities.addAll(template.getRebuildEntities());
                    if (template.hasEntityToRebuild()) {
                        template.executeRebuildAction();
                        log.info(String.format("%s enabled: %s entities to rebuild", c.getSimpleName(),
                                rebuildEntities.toString()));
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            return rebuildEntities;
        }
    }

    abstract class RebuildEntitiesTemplate {

        protected boolean hasEntityToRebuild;

        public abstract Set<BusinessEntity> getRebuildEntities();

        public boolean hasEntityToRebuild() {
            return hasEntityToRebuild;
        }

        protected void setEntityToRebuild() {
            hasEntityToRebuild = true;
        }

        public void executeRebuildAction() {
        }
    }

    class RebuildOnDLVersionTemplate extends RebuildEntitiesTemplate {

        @Override
        public Set<BusinessEntity> getRebuildEntities() {
            Set<BusinessEntity> rebuildEntities = new HashSet<>();
            String currentBuildNumber = configuration.getDataCloudBuildNumber();
            DataCollection dataCollection = dataCollectionProxy.getDefaultDataCollection(customerSpace.toString());
            if (dataCollection != null && dataCollection.getDataCloudBuildNumber() != null
                    && !dataCollection.getDataCloudBuildNumber().equals(currentBuildNumber)) {

                rebuildEntities.add(BusinessEntity.Account);
                setEntityToRebuild();
            }
            return rebuildEntities;
        }

        @Override
        public void executeRebuildAction() {
            ArrayNode systemActionsNode = reportJson.putArray(ReportPurpose.SYSTEM_ACTIONS.getKey());
            systemActionsNode.add("Rebuild due to Data Cloud Version Changed");
            putObjectInContext(ReportPurpose.PROCESS_ANALYZE_RECORDS_SUMMARY.getKey(), reportJson);
        }
    }

    class RebuildOnDeleteJobTemplate extends RebuildEntitiesTemplate {

        @Override
        public Set<BusinessEntity> getRebuildEntities() {
            Set<BusinessEntity> rebuildEntities = new HashSet<>();
            try {
                List<Job> deleteJobs = getDeleteJobs();
                for (Job job : deleteJobs) {
                    String str = job.getOutputs().get(WorkflowContextConstants.Outputs.IMPACTED_BUSINESS_ENTITIES);
                    if (StringUtils.isEmpty(str)) {
                        continue;
                    }
                    List<String> entityStrs = Arrays.asList(str.split(","));
                    rebuildEntities
                            .addAll(entityStrs.stream().map(BusinessEntity::valueOf).collect(Collectors.toSet()));
                    setEntityToRebuild();
                }
                return rebuildEntities;
            } catch (Exception e) {
                log.error("Failed to set rebuild entities based on delete actions.", e);
                throw new RuntimeException(e);
            }
        }
    }
}
