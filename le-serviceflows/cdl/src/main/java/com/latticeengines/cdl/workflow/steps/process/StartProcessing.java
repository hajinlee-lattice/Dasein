package com.latticeengines.cdl.workflow.steps.process;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
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
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
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
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;
import com.latticeengines.workflow.exposed.user.WorkflowUser;

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

        String evaluationDate = periodProxy.getEvaluationDate(customerSpace.toString());
        putStringValueInContext(CDL_EVALUATION_DATE, evaluationDate);
        putLongValueInContext(PA_TIMESTAMP, System.currentTimeMillis());

        // get current active collection status
        DataCollectionStatus detail = dataCollectionProxy.getOrCreateDataCollectionStatus(customerSpace.toString(),
                null);
        detail.setEvaluationDate(evaluationDate);
        detail.setApsRollingPeriod(configuration.getApsRollingPeriod());
        log.info("StartProcessing step: dataCollection Status is " + JsonUtils.serialize(detail));
        if (detail.getDateMap() == null) {
            generateDateValueForCollectionDetail(detail);
        }
        putObjectInContext(CDL_COLLECTION_STATUS, detail);

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

        List<Action> attrMgmtActions = getAttrManagementActions(actions);
        List<Action> accountAttrActions = getAttrManagementActionsForAccount(attrMgmtActions);
        List<Action> contactAttrActions = getAttrManagementActionsForContact(attrMgmtActions);
        grapherContext.setHasAccountAttrLifeCycleChange(CollectionUtils.isNotEmpty(accountAttrActions));
        grapherContext.setHasContactAttrLifeCycleChange(CollectionUtils.isNotEmpty(contactAttrActions));
        List<Action> purchaseMetricsActions = getPurchaseMetricsActions(actions);
        grapherContext.setPurchaseMetricsChanged(CollectionUtils.isNotEmpty(purchaseMetricsActions));

        List<Action> ratingActions = getRatingRelatedActions(actions);
        List<String> segments = getActionImpactedSegmentNames(ratingActions);
        grapherContext.setHasRatingEngineChange(
                CollectionUtils.isNotEmpty(ratingActions) || CollectionUtils.isNotEmpty(segments));

        putObjectInContext(CHOREOGRAPHER_CONTEXT_KEY, grapherContext);
    }

    protected Set<BusinessEntity> getImpactedEntities() {
        return RebuildEntitiesProvider.getRebuildEntities(this);
    }

    private boolean checkDataCloudChange() {
        boolean changed = false;
        String currentBuildNumber = null, statusBuildNumber = null;
        if (Boolean.TRUE.equals(configuration.getIgnoreDataCloudChange())) {
            log.info("Specified to ignore data cloud change.");
        } else {
            currentBuildNumber = configuration.getDataCloudBuildNumber();
            DataCollectionStatus status = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
            if (status != null
                    && (status.getDataCloudBuildNumber() == null
                    || !status.getDataCloudBuildNumber().equals(currentBuildNumber))
                    && hasAccountBatchStore()) {
                statusBuildNumber = status.getDataCloudBuildNumber();
                changed = true;
            }
            log.info("Data cloud changed?=" + changed + " current LDC build number=" + currentBuildNumber
                    + ", the LDC builder number in data collection status="
                    + (status == null ? "" : status.getDataCloudBuildNumber()));
            if (status != null) {
                status.setDataCloudBuildNumber(currentBuildNumber);
                putObjectInContext(CDL_COLLECTION_STATUS, status);
            }
        }

        if (changed) {
            DataCloudVersion currentVersion = DataCloudVersion.parseBuildNumber(currentBuildNumber);
            DataCloudVersion statusVersion = DataCloudVersion.parseBuildNumber(statusBuildNumber);
            if (DataCloudVersion.versionComparator.compare(currentVersion, statusVersion) != 0) {
                createSystemAction(ActionType.DATA_CLOUD_CHANGE, ActionType.DATA_CLOUD_CHANGE.getDisplayName());
                DataCollectionStatus status = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
                Long PATime = getLongValueFromContext(PA_TIMESTAMP);
                Map<Category, Long> dateMap = status.getDateMap();
                dateMap.put(Category.FIRMOGRAPHICS, PATime);
                dateMap.put(Category.GROWTH_TRENDS, PATime);
                dateMap.put(Category.INTENT, PATime);
                dateMap.put(Category.ONLINE_PRESENCE, PATime);
                dateMap.put(Category.TECHNOLOGY_PROFILE, PATime);
                dateMap.put(Category.WEBSITE_KEYWORDS, PATime);
                dateMap.put(Category.WEBSITE_PROFILE, PATime);
                dateMap.put(Category.ACCOUNT_ATTRIBUTES, PATime);
                putObjectInContext(CDL_COLLECTION_STATUS, status);
            } else if (StringUtils.compare(
                    currentVersion.getRefreshVersionVersion(), statusVersion.getRefreshVersionVersion()) != 0) {
                createSystemAction(ActionType.INTENT_CHANGE, ActionType.INTENT_CHANGE.getDisplayName());
                DataCollectionStatus status = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
                Long PATime = getLongValueFromContext(PA_TIMESTAMP);
                Map<Category, Long> dateMap = status.getDateMap();
                dateMap.put(Category.INTENT, PATime);
                putObjectInContext(CDL_COLLECTION_STATUS, status);
            }
        }

        return changed;
    }

    private void createSystemAction(ActionType type, String description) {
        Action action = new Action();
        action.setType(type);
        action.setCreated(new Date(System.currentTimeMillis()));
        action.setDescription(description);
        action.setActionInitiator(WorkflowUser.DEFAULT_USER.name());
        action.setOwnerId(configuration.getOwnerId());
        action = actionProxy.createAction(customerSpace.getTenantId(), action);
        List<Long> actionIds = configuration.getActionIds();
        actionIds.add(action.getPid());
        configuration.setActionIds(actionIds);
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
            AttrConfigLifeCycleChangeConfiguration lifeCycleChangeConfiguration = (AttrConfigLifeCycleChangeConfiguration) action
                    .getActionConfiguration();
            String categoryName = lifeCycleChangeConfiguration.getCategoryName();
            return !Category.CONTACT_ATTRIBUTES.equals(Category.fromName(categoryName));
        }).collect(Collectors.toList());
    }

    private List<Action> getAttrManagementActionsForContact(List<Action> actions) {
        return actions.stream().filter(action -> {
            AttrConfigLifeCycleChangeConfiguration lifeCycleChangeConfiguration = (AttrConfigLifeCycleChangeConfiguration) action
                    .getActionConfiguration();
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
        if (CollectionUtils.isNotEmpty(actions)) {
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

    private List<Action> getDeleteActions() {
        List<Action> actionList = getActions();
        return actionList.stream().filter(action -> ActionType.CDL_OPERATION_WORKFLOW.equals(action.getType()))
                .collect(Collectors.toList());
    }

    private List<Action> getImportActions() {
        List<Action> actionList = getActions();
        return actionList.stream().filter(action -> ActionType.CDL_DATAFEED_IMPORT_WORKFLOW.equals(action.getType()))
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
        if (entityImportsMap.containsKey(BusinessEntity.Account)) {
            DataCollectionStatus status = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
            Long PATime = getLongValueFromContext(PA_TIMESTAMP);
            Map<Category, Long> dateMap = status.getDateMap();
            dateMap.put(Category.ACCOUNT_ATTRIBUTES, PATime);
            putObjectInContext(CDL_COLLECTION_STATUS, status);
        }
        if (entityImportsMap.containsKey(BusinessEntity.Contact)) {
            DataCollectionStatus status = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
            Long PATime = getLongValueFromContext(PA_TIMESTAMP);
            Map<Category, Long> dateMap = status.getDateMap();
            dateMap.put(Category.CONTACT_ATTRIBUTES, PATime);
            putObjectInContext(CDL_COLLECTION_STATUS, status);
        }
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
                ImportActionConfiguration importActionConfiguration = (ImportActionConfiguration) action
                        .getActionConfiguration();
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

    private void generateDateValueForCollectionDetail(DataCollectionStatus detail) {
        Long PATime = getLongValueFromContext(PA_TIMESTAMP);
        Map<Category, Long> dateMap = new HashMap<>();
        detail.setDateMap(dateMap);
        dateMap.put(Category.FIRMOGRAPHICS, PATime);
        dateMap.put(Category.GROWTH_TRENDS, PATime);
        dateMap.put(Category.INTENT, PATime);
        dateMap.put(Category.ONLINE_PRESENCE, PATime);
        dateMap.put(Category.TECHNOLOGY_PROFILE, PATime);
        dateMap.put(Category.WEBSITE_KEYWORDS, PATime);
        dateMap.put(Category.WEBSITE_PROFILE, PATime);
        dateMap.put(Category.ACCOUNT_ATTRIBUTES, PATime);
        dateMap.put(Category.CONTACT_ATTRIBUTES, PATime);
        dateMap.put(Category.RATING, PATime);
        dateMap.put(Category.PRODUCT_SPEND, PATime);

    }

    public static class RebuildEntitiesProvider {
        static Set<BusinessEntity> getRebuildEntities(StartProcessing st) {
            Set<BusinessEntity> rebuildEntities = new HashSet<>();
            Collection<Class<? extends RebuildEntitiesTemplate>> decrators = Collections
                    .singletonList(RebuildOnDeleteJobTemplate.class);
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
            // do nothing
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
