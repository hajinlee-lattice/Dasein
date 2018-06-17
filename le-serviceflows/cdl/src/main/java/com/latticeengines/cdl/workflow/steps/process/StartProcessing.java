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
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ChoreographerContext;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecutionJobType;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedImport;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.AttrConfigLifeCycleChangeConfiguration;
import com.latticeengines.domain.exposed.pls.CleanupActionConfiguration;
import com.latticeengines.domain.exposed.pls.ImportActionConfiguration;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineActionConfiguration;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.SegmentActionConfiguration;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.ProcessAnalyzeWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessStepConfiguration;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.domain.exposed.workflow.BaseWrapperStepConfiguration;
import com.latticeengines.domain.exposed.workflow.BaseWrapperStepConfiguration.Phase;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.proxy.exposed.cdl.ActionProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.cdl.PeriodProxy;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("startProcessing")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class StartProcessing extends BaseWorkflowStep<ProcessStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(StartProcessing.class);

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private DataFeedProxy dataFeedProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private PeriodProxy periodProxy;

    @Inject
    private ActionProxy actionProxy;

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    private CustomerSpace customerSpace;
    private DataCollection.Version activeVersion;
    private DataCollection.Version inactiveVersion;
    private InternalResourceRestApiProxy internalResourceProxy;
    private ObjectNode reportJson;
    private ChoreographerContext grapherContext = new ChoreographerContext();;

    @PostConstruct
    public void init() {
        internalResourceProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
    }

    // for Spring
    public StartProcessing() {
    }

    @VisibleForTesting
    StartProcessing(DataCollectionProxy dataCollectionProxy, InternalResourceRestApiProxy internalResourceProxy,
                    ActionProxy actionProxy, CustomerSpace customerSpace) {
        this.dataCollectionProxy = dataCollectionProxy;
        this.internalResourceProxy = internalResourceProxy;
        this.actionProxy = actionProxy;
        this.customerSpace = customerSpace;
        this.reportJson = JsonUtils.createObjectNode();
    }

    @Override
    public void execute() {
        clearExecutionContext();
        customerSpace = configuration.getCustomerSpace();
        addActionAssociateTables();
        determineVersions();

        DataFeedExecution execution = dataFeedProxy.startExecution(customerSpace.toString(),
                DataFeedExecutionJobType.PA, jobId);
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

        String evaluationDate = periodProxy.getEvaluationDate(customerSpace.toString());
        putStringValueInContext(CDL_EVALUATION_DATE, evaluationDate);

        // get current active collection status
        DataCollectionStatus detail = dataCollectionProxy
                .getOrCreateDataCollectionStatus(customerSpace.toString(), null);
        detail.setEvaluationDate(evaluationDate);
        detail.setDataCloudBuildNumber(configuration.getDataCloudBuildNumber());
        detail.setApsRollingPeriod(configuration.getApsRollingPeriod());
        log.info("StartProcessing step: dataCollection Status is " + JsonUtils.serialize(detail));
        putObjectInContext(CDL_COLLECTION_STATUS, detail);

        createReportJson();
        setupInactiveVersion();
        setGrapherContext();
        // clearPhaseForRetry();
    }

    @SuppressWarnings("unused")
    private void clearPhaseForRetry() {
        Map<String, BaseStepConfiguration> stepConfigMap = getStepConfigMapInWorkflow("", "",
                ProcessAnalyzeWorkflowConfiguration.class);
        if (stepConfigMap.isEmpty()) {
            log.info("stepConfigMap is Empty!!!");
        }
        stepConfigMap.entrySet().stream().filter(e -> e.getValue() instanceof BaseWrapperStepConfiguration)//
                .forEach(e -> {
                    log.info("enabling step:" + e.getKey());
                    e.getValue().setSkipStep(false);
                    (BaseWrapperStepConfiguration.class.cast(e.getValue())).setPhase(Phase.PRE_PROCESSING);
                    putObjectInContext(e.getKey(), e.getValue());
                });
    }

    protected void setGrapherContext() {
        grapherContext.setDataCloudChanged(checkDataCloudChange());
        Set<BusinessEntity> impactedEntities = getImpactedEntities();
        grapherContext.setJobImpactedEntities(impactedEntities);
        List<Action> actions = getActions();

        // TODO: separate out actions by entity
        List<Action> attrMgmtActions = getAttrManagementActions(actions);
        List<Action> accountAttrActions = getAttrManagementActionsForAccount(attrMgmtActions);
        List<Action> contactAttrActions = getAttrManagementActionsForContact(attrMgmtActions);
        grapherContext.setHasAccountAttrLifeCycleChange(CollectionUtils.isNotEmpty(accountAttrActions));
        grapherContext.setHasContactAttrLifeCycleChange(CollectionUtils.isNotEmpty(contactAttrActions));
        List<Action> purchaseMetricsActions = getPurchaseMetricsActions(actions);
        grapherContext.setPurchaseMetricsChanged(CollectionUtils.isNotEmpty(purchaseMetricsActions));

        // FIXME: not really working before persisting action configuration
        List<Action> ratingActions = getRatingRelatedActions(actions);
        grapherContext.setHasRatingEngineChange(CollectionUtils.isNotEmpty(ratingActions));
        List<String> segments = getActionImpactedSegmentNames(ratingActions);
        grapherContext.setActionImpactedAIRatingEngines(getActionImpactedAIEngineIds(ratingActions, segments));
        grapherContext.setActionImpactedRuleRatingEngines(getActionImpactedRuleEngineIds(ratingActions, segments));

        putObjectInContext(CHOREOGRAPHER_CONTEXT_KEY, grapherContext);
    }

    protected Set<BusinessEntity> getImpactedEntities() {
        return RebuildEntitiesProvider.getRebuildEntities(this);
    }

    private boolean checkDataCloudChange() {
        boolean changed = false;
        if (Boolean.TRUE.equals(configuration.getIgnoreDataCloudChange())) {
            log.info("Specified to ignore data cloud change.");
        } else {
            String currentBuildNumber = configuration.getDataCloudBuildNumber();
            DataCollection dataCollection = dataCollectionProxy.getDefaultDataCollection(customerSpace.toString());
            if (dataCollection != null
                    && (dataCollection.getDataCloudBuildNumber() == null
                    || !dataCollection.getDataCloudBuildNumber().equals(currentBuildNumber))
                    && hasAccountBatchStore()) {
                changed = true;
            }
            log.info("Data cloud changed?=" + changed + " current LDC build number=" + currentBuildNumber
                    + ", the LDC builder number in data collection="
                    + (dataCollection == null ? "" : dataCollection.getDataCloudBuildNumber()));
        }
        return changed;
    }

    boolean hasAccountBatchStore() {
        String accountTableName = dataCollectionProxy.getTableName(customerSpace.toString(), //
                TableRoleInCollection.ConsolidatedAccount, activeVersion);
        boolean hasBatchStore = StringUtils.isNotBlank(accountTableName);
        log.info("Account batch store exist=" + hasBatchStore);
        return hasBatchStore;
    }

    protected List<Action> getActions() {
        if (CollectionUtils.isEmpty(configuration.getActionIds())) {
            return Collections.emptyList();
        }
        List<Action> actions = actionProxy.getActionsByPids(customerSpace.toString(), configuration.getActionIds());
        if (actions == null) {
            actions = Collections.emptyList();
        }
        return actions;
    }

    private List<Action> getAttrManagementActions(List<Action> actions) {
        return actions.stream().filter(action -> ActionType.getAttrManagementTypes().contains(action.getType()))
                .collect(Collectors.toList());
    }

    private List<Action> getAttrManagementActionsForAccount(List<Action> actions) {
        return actions.stream().filter(action -> {
            AttrConfigLifeCycleChangeConfiguration lifeCycleChangeConfiguration =
                    (AttrConfigLifeCycleChangeConfiguration) action.getActionConfiguration();
            String categoryName = lifeCycleChangeConfiguration.getCategoryName();
            return !Category.CONTACT_ATTRIBUTES.equals(Category.fromName(categoryName));
        }).collect(Collectors.toList());
    }

    private List<Action> getAttrManagementActionsForContact(List<Action> actions) {
        return actions.stream().filter(action -> {
            AttrConfigLifeCycleChangeConfiguration lifeCycleChangeConfiguration =
                    (AttrConfigLifeCycleChangeConfiguration) action.getActionConfiguration();
            String categoryName = lifeCycleChangeConfiguration.getCategoryName();
            return Category.CONTACT_ATTRIBUTES.equals(Category.fromName(categoryName));
        }).collect(Collectors.toList());
    }

    protected List<Action> getRatingRelatedActions(List<Action> actions) {
        return actions.stream().filter(action -> ActionType.getRatingRelatedTypes().contains(action.getType()))
                .collect(Collectors.toList());
    }

    private List<Action> getPurchaseMetricsActions(List<Action> actions) {
        return actions.stream().filter(action -> action.getType() == ActionType.ACTIVITY_METRICS_CHANGE)
                .collect(Collectors.toList());
    }

    protected List<String> getActionImpactedSegmentNames(List<Action> actions) {
        List<String> segmentNames = new ArrayList<>();
        if (actions != null) {
            for (Action action : actions) {
                if (ActionType.METADATA_SEGMENT_CHANGE.equals(action.getType())) {
                    SegmentActionConfiguration configuration = (SegmentActionConfiguration) action
                            .getActionConfiguration();
                    if (configuration != null) {
                        segmentNames.add(configuration.getSegmentName());
                    }
                }
            }
        }
        return segmentNames;
    }

    protected List<String> getActionImpactedAIEngineIds(List<Action> actions, Collection<String> segments) {
        return getActionImpactedEngineIds(actions, segments,
                Arrays.asList(RatingEngineType.CUSTOM_EVENT, RatingEngineType.CROSS_SELL));
    }

    protected List<String> getActionImpactedRuleEngineIds(List<Action> actions, Collection<String> segments) {
        return getActionImpactedEngineIds(actions, segments, Collections.singletonList(RatingEngineType.RULE_BASED));
    }

    private List<String> getActionImpactedEngineIds(List<Action> actions, Collection<String> segments,
            Collection<RatingEngineType> types) {
        List<String> engineIds = new ArrayList<>();
        if (actions != null) {
            String customerSpace = configuration.getCustomerSpace().toString();
            for (Action action : actions) {
                if (ActionType.RATING_ENGINE_CHANGE.equals(action.getType())) {
                    RatingEngineActionConfiguration configuration = (RatingEngineActionConfiguration) action
                            .getActionConfiguration();
                    if (configuration != null) {
                        String engineId = configuration.getRatingEngineId();
                        RatingEngine ratingEngine = ratingEngineProxy.getRatingEngine(customerSpace, engineId);
                        if (ratingEngine != null //
                                && !Boolean.TRUE.equals(ratingEngine.getDeleted()) //
                                && !Boolean.TRUE.equals(ratingEngine.getJustCreated())) {
                            if (types.contains(ratingEngine.getType())
                                    || segments.contains(ratingEngine.getSegment().getName())) {
                                String logMsg = String.format(
                                        "Found a rating engine change action related to %s engine %s (%s): %s",
                                        ratingEngine.getType().name(), ratingEngine.getId(), ratingEngine.getDisplayName(),
                                        JsonUtils.serialize(action));
                                log.info(logMsg);
                                engineIds.add(ratingEngine.getId());
                            }
                        }
                    }
                }
            }
        }
        log.info("Found action impacted rating engines of type " + types + ": " + engineIds);
        return engineIds;
    }

    private List<Action> getDeleteActions() {
        List<Action> actionList = getActions();
        return actionList.stream()
                .filter(action -> ActionType.CDL_OPERATION_WORKFLOW.equals(action.getType()))
                .collect(Collectors.toList());
    }

    private List<Action> getImportActions() {
        List<Action> actionList = getActions();
        return actionList.stream()
                .filter(action -> ActionType.CDL_DATAFEED_IMPORT_WORKFLOW.equals(action.getType()))
                .collect(Collectors.toList());
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
        execution.getImports().forEach(imp -> {
            BusinessEntity entity = BusinessEntity.valueOf(imp.getEntity());
            entityImportsMap.putIfAbsent(entity, new ArrayList<>());
            entityImportsMap.get(entity).add(imp);
        });
        putObjectInContext(CONSOLIDATE_INPUT_IMPORTS, entityImportsMap);
    }

    private void addActionAssociateTables() {
        List<Action> actionList = getImportActions();
        if (CollectionUtils.isNotEmpty(actionList)) {
            for (Action action : actionList) {
                if (action.getActionConfiguration() == null) {
                    log.warn(String.format("Action %d does not have a import configuration, may need re-import.",
                            action.getPid()));
                    continue;
                }
                ImportActionConfiguration importActionConfiguration =
                        (ImportActionConfiguration) action.getActionConfiguration();
                String taskId = importActionConfiguration.getDataFeedTaskId();
                if (StringUtils.isEmpty(taskId)) {
                    continue;
                }
                if (importActionConfiguration.getImportCount() == 0) {
                    continue;
                }
                List<String> tables = importActionConfiguration.getRegisteredTables();
                if (CollectionUtils.isEmpty(tables)) {
                    log.warn(String.format("Action %d doesn't have table to be registered.", action.getPid()));
                    continue;
                }
                dataFeedProxy.addTablesToQueue(customerSpace.toString(), taskId, tables);
            }
        }
    }

    private void createReportJson() {
        reportJson = JsonUtils.createObjectNode();
        putObjectInContext(ReportPurpose.PROCESS_ANALYZE_RECORDS_SUMMARY.getKey(), reportJson);
    }

    private void setupInactiveVersion() {
        for (TableRoleInCollection role : TableRoleInCollection.values()) {
            List<String> tableNames = dataCollectionProxy.getTableNames(customerSpace.toString(), role,
                    inactiveVersion);
            if (CollectionUtils.isNotEmpty(tableNames)) {
                List<String> activeTableNames = dataCollectionProxy.getTableNames(customerSpace.toString(), role,
                        activeVersion);
                Set<String> activeTableNameSet = new HashSet<>();
                activeTableNames.forEach(t -> {
                    activeTableNameSet.add(t.toLowerCase());
                });
                for (String tableName : tableNames) {
                    if (activeTableNameSet.contains(tableName.toLowerCase())) {
                        log.info("Unlink table " + tableName + " as " + role + " in " + inactiveVersion);
                        dataCollectionProxy.unlinkTable(customerSpace.toString(), tableName, role, inactiveVersion);
                    } else {
                        log.info("Removing table " + tableName + " as " + role + " in " + inactiveVersion);
                        metadataProxy.deleteTable(customerSpace.toString(), tableName);
                    }
                }
            }
        }
        log.info("Removing stats in " + inactiveVersion);
        dataCollectionProxy.removeStats(customerSpace.toString(), inactiveVersion);
    }

    public static class RebuildEntitiesProvider {
        static Set<BusinessEntity> getRebuildEntities(StartProcessing st) {
            Set<BusinessEntity> rebuildEntities = new HashSet<>();
            Collection<Class<? extends RebuildEntitiesTemplate>> decrators =
                    Collections.singletonList(RebuildOnDeleteJobTemplate.class);
            for (Class<? extends RebuildEntitiesTemplate> c : decrators) {
                try {
                    RebuildEntitiesTemplate template = c.getDeclaredConstructor(StartProcessing.class).newInstance(st);
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

    class RebuildOnDeleteJobTemplate extends RebuildEntitiesTemplate {

        @Override
        public Set<BusinessEntity> getRebuildEntities() {
            Set<BusinessEntity> rebuildEntities = new HashSet<>();
            try {
                List<Action> deleteActions = getDeleteActions();
                for (Action action : deleteActions) {
                    CleanupActionConfiguration cleanupActionConfiguration = (CleanupActionConfiguration) action
                            .getActionConfiguration();
                    rebuildEntities.addAll(cleanupActionConfiguration.getImpactEntities());
                }
                return rebuildEntities;
            } catch (Exception e) {
                log.error("Failed to set rebuild entities based on delete actions.", e);
                throw new RuntimeException(e);
            }
        }
    }
}
