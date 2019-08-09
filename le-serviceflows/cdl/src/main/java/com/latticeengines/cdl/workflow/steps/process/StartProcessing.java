package com.latticeengines.cdl.workflow.steps.process;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
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
import com.latticeengines.domain.exposed.cdl.AttributeLimit;
import com.latticeengines.domain.exposed.cdl.ChoreographerContext;
import com.latticeengines.domain.exposed.cdl.DataLimit;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatusDetail;
import com.latticeengines.domain.exposed.metadata.MigrationTrack;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecutionJobType;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedImport;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.AttrConfigLifeCycleChangeConfiguration;
import com.latticeengines.domain.exposed.pls.CleanupActionConfiguration;
import com.latticeengines.domain.exposed.pls.ImportActionConfiguration;
import com.latticeengines.domain.exposed.pls.RatingEngineActionConfiguration;
import com.latticeengines.domain.exposed.pls.SegmentActionConfiguration;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.ProcessAnalyzeWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessStepConfiguration;
import com.latticeengines.domain.exposed.util.DataCollectionStatusUtils;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.domain.exposed.workflow.BaseWrapperStepConfiguration;
import com.latticeengines.domain.exposed.workflow.BaseWrapperStepConfiguration.Phase;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.proxy.exposed.cdl.ActionProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.cdl.PeriodProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("startProcessing")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class StartProcessing extends BaseWorkflowStep<ProcessStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(StartProcessing.class);
    private static final String SYSTEM_ACTION_INITIATOR = "system@lattice-engines.com";

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

    @Value("${yarn.pls.url}")
    private String internalResourceHostPort;

    private CustomerSpace customerSpace;
    private DataCollection.Version activeVersion;
    private DataCollection.Version inactiveVersion;
    private ObjectNode reportJson;
    private ChoreographerContext grapherContext = new ChoreographerContext();
    private Long newTransactionCount = 0L;
    private boolean migrationMode;

    @PostConstruct
    public void init() {
    }

    // for Spring
    public StartProcessing() {
    }

    @VisibleForTesting
    StartProcessing(DataCollectionProxy dataCollectionProxy, ActionProxy actionProxy, CustomerSpace customerSpace) {
        this.dataCollectionProxy = dataCollectionProxy;
        this.actionProxy = actionProxy;
        this.customerSpace = customerSpace;
        this.reportJson = JsonUtils.createObjectNode();
    }

    @Override
    public void execute() {
        clearExecutionContext(getRenewableCtxKeys());
        customerSpace = configuration.getCustomerSpace();
        addActionAssociateTables();
        determineVersions();
        setMigrationMode();

        if (migrationMode) {
            log.info("Tenant {} is marked as STARTED in migration table. Running with migration mode", customerSpace);
            verifyActiveDataCollectionVersion();
            unlinkTables();
        }

        String tenantName = CustomerSpace.shortenCustomerSpace(customerSpace.toString());
        log.info("tenantName is :" + tenantName);
        if (tenantName.equalsIgnoreCase("QA_Atlas_Auto_Refresh_2")) {
            List<String> warningMessages = getListObjectFromContext(PROCESS_ANALYTICS_WARNING_KEY, String.class);
            if (CollectionUtils.isEmpty(warningMessages)) {
                warningMessages = new LinkedList<>();
            }
            warningMessages.add("this warning message from tenant QA_Atlas_Auto_Refresh_2, just for testing.");
            putObjectInContext(PROCESS_ANALYTICS_WARNING_KEY, warningMessages);
        }

        if (!hasKeyInContext(FULL_REMATCH_PA)) {
            boolean fullRematch = Boolean.TRUE.equals(configuration.isFullRematch());
            putObjectInContext(FULL_REMATCH_PA, fullRematch);
        }

        if (!hasKeyInContext(NEW_RECORD_CUT_OFF_TIME)) {
            putLongValueInContext(NEW_RECORD_CUT_OFF_TIME, System.currentTimeMillis());
        }

        putObjectInContext(SKIP_PUBLISH_PA_TO_S3, configuration.isSkipPublishToS3());

        String evaluationDate = periodProxy.getEvaluationDate(customerSpace.toString());
        putStringValueInContext(CDL_EVALUATION_DATE, evaluationDate);
        if (!hasKeyInContext(PA_TIMESTAMP)) {
            putLongValueInContext(PA_TIMESTAMP, System.currentTimeMillis());
        }
        putObjectInContext(PA_SKIP_ENTITIES, configuration.getSkipEntities());

        setupDataCollectionStatus(evaluationDate);

        updateDataFeed();

        createReportJson();
        setupInactiveVersion();
        setGrapherContext();
        resetEntityMatchFlagsForRetry();
        setAttributeQuotaLimit();
        setDataQuotaLimit();
        reachTransactionLimit();
    }

    private void updateDataFeed() {
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
    }

    private void setupDataCollectionStatus(String evaluationDate) {
        // get current active collection status
        DataCollectionStatus dcStatus = dataCollectionProxy.getOrCreateDataCollectionStatus(customerSpace.toString(),
                null);
        dcStatus.setEvaluationDate(evaluationDate);
        dcStatus.setApsRollingPeriod(configuration.getApsRollingPeriod());
        log.info("StartProcessing step: dataCollection Status is " + JsonUtils.serialize(dcStatus));
        if (MapUtils.isEmpty(dcStatus.getDateMap())) {
            dcStatus = DataCollectionStatusUtils.initDateMap(dcStatus, getLongValueFromContext(PA_TIMESTAMP));
        }
        putObjectInContext(CDL_COLLECTION_STATUS, dcStatus);

        DataCollectionStatus inactiveStatus = dataCollectionProxy
                .getOrCreateDataCollectionStatus(customerSpace.toString(), inactiveVersion);
        if (inactiveStatus != null && configuration.getDataCloudBuildNumber() != null) {
            inactiveStatus.setDataCloudBuildNumber(configuration.getDataCloudBuildNumber());
            dataCollectionProxy.saveOrUpdateDataCollectionStatus(customerSpace.toString(), inactiveStatus,
                    inactiveVersion);
        }

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
                    ((BaseWrapperStepConfiguration) e.getValue()).setPhase(Phase.PRE_PROCESSING);
                    putObjectInContext(e.getKey(), e.getValue());
                });
    }

    void setGrapherContext() {
        List<Action> actions = getActions();
        grapherContext.setDataCloudChanged(checkDataCloudChange());
        grapherContext.setEntitiesRebuildDueToActions(getEntitiesShouldRebuildByActions());

        List<Action> attrMgmtActions = getAttrManagementActions(actions);
        List<Action> accountAttrActions = getAttrManagementActionsForAccount(attrMgmtActions);
        List<Action> contactAttrActions = getAttrManagementActionsForContact(attrMgmtActions);
        grapherContext.setHasAccountAttrLifeCycleChange(CollectionUtils.isNotEmpty(accountAttrActions));
        grapherContext.setHasContactAttrLifeCycleChange(CollectionUtils.isNotEmpty(contactAttrActions));
        List<Action> purchaseMetricsActions = getPurchaseMetricsActions(actions);
        grapherContext.setPurchaseMetricsChanged(CollectionUtils.isNotEmpty(purchaseMetricsActions));
        List<Action> businessCalenderChangedActions = getBusinessCalendarChangedActions(actions);
        grapherContext.setBusinessCalenderChanged(CollectionUtils.isNotEmpty(businessCalenderChangedActions));

        List<String> engineIds = getActionImpactedEngineIds(actions);
        List<String> segments = getActionImpactedSegmentNames(actions);
        grapherContext.setHasRatingEngineChange(
                CollectionUtils.isNotEmpty(engineIds) || CollectionUtils.isNotEmpty(segments));
        putObjectInContext(ACTION_IMPACTED_ENGINES, engineIds);
        putObjectInContext(ACTION_IMPACTED_SEGMENTS, segments);

        grapherContext.setFullRematch(Boolean.TRUE.equals(getObjectFromContext(FULL_REMATCH_PA, Boolean.class)));

        putObjectInContext(CHOREOGRAPHER_CONTEXT_KEY, grapherContext);
    }

    Set<BusinessEntity> getEntitiesShouldRebuildByActions() {
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
                    || DataCollectionStatusDetail.NOT_SET.equals(status.getDataCloudBuildNumber())
                    || !status.getDataCloudBuildNumber().equals(currentBuildNumber))
                    && hasAccountBatchStore()) {
                statusBuildNumber = DataCollectionStatusDetail.NOT_SET.equals(status.getDataCloudBuildNumber()) ? null
                        : status.getDataCloudBuildNumber();
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
            if (statusVersion == null
                    || DataCloudVersion.versionComparator.compare(currentVersion, statusVersion) != 0) {
                createSystemAction(ActionType.DATA_CLOUD_CHANGE, ActionType.DATA_CLOUD_CHANGE.getDisplayName());
                DataCollectionStatus status = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
                status = DataCollectionStatusUtils.updateTimeForDCChange(status, getLongValueFromContext(PA_TIMESTAMP));
                putObjectInContext(CDL_COLLECTION_STATUS, status);
            } else if (StringUtils.compare(
                    currentVersion == null ? null : currentVersion.getRefreshVersionVersion(),
                    statusVersion.getRefreshVersionVersion()) != 0) {
                createSystemAction(ActionType.INTENT_CHANGE, ActionType.INTENT_CHANGE.getDisplayName());
                DataCollectionStatus status = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
                status = DataCollectionStatusUtils.updateTimeForCategoryChange(status,
                        getLongValueFromContext(PA_TIMESTAMP), Category.INTENT);
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
        action.setActionInitiator(SYSTEM_ACTION_INITIATOR);
        action.setOwnerId(configuration.getOwnerId());
        action = actionProxy.createAction(customerSpace.getTenantId(), action);
        List<Long> systemActionIds = getListObjectFromContext(SYSTEM_ACTION_IDS, Long.class);
        if (CollectionUtils.isEmpty(systemActionIds)) {
            systemActionIds = new ArrayList<>();
        }
        systemActionIds.add(action.getPid());
        putObjectInContext(SYSTEM_ACTION_IDS, systemActionIds);
        log.info(String.format("System actions are: %s", JsonUtils.serialize(systemActionIds)));
        String actionIdStr = configuration.getInputProperties().get(WorkflowContextConstants.Inputs.ACTION_IDS);
        List<Long> actionIds;
        if (StringUtils.isNotEmpty(actionIdStr)) {
            List<?> rawActionIds = JsonUtils.deserialize(actionIdStr, List.class);
            actionIds = JsonUtils.convertList(rawActionIds, Long.class);
        } else {
            actionIds = new ArrayList<>();
        }
        actionIds.add(action.getPid());
        String actionIdString = JsonUtils.serialize(actionIds);
        configuration.getInputProperties().put(WorkflowContextConstants.Inputs.ACTION_IDS, actionIdString);
        configuration.setActionIds(actionIds);
        if (jobId != null) {
            WorkflowJob job = workflowJobEntityMgr.findByWorkflowId(jobId);
            if (job != null) {
                job.getInputContext().put(WorkflowContextConstants.Inputs.ACTION_IDS, actionIdString);
                workflowJobEntityMgr.updateInput(job);
            }
        }
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

    private List<Action> getPurchaseMetricsActions(List<Action> actions) {
        return actions.stream().filter(action -> action.getType() == ActionType.ACTIVITY_METRICS_CHANGE)
                .collect(Collectors.toList());
    }

    private List<Action> getBusinessCalendarChangedActions(List<Action> actions) {
        return actions.stream().filter(action -> action.getType() == ActionType.BUSINESS_CALENDAR_CHANGE)
                .collect(Collectors.toList());
    }

    private List<String> getActionImpactedEngineIds(List<Action> actions) {
        List<String> engineIds = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(actions)) {
            for (Action action: actions) {
                if (ActionType.getRatingRelatedTypes().contains(action.getType())) {
                    RatingEngineActionConfiguration configuration = //
                            (RatingEngineActionConfiguration) action.getActionConfiguration();
                    engineIds.add(configuration.getRatingEngineId());
                }
            }
        }
        return engineIds;
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
            status = DataCollectionStatusUtils.updateTimeForCategoryChange(status,
                    getLongValueFromContext(PA_TIMESTAMP), Category.ACCOUNT_ATTRIBUTES);
            putObjectInContext(CDL_COLLECTION_STATUS, status);
        }
        if (entityImportsMap.containsKey(BusinessEntity.Contact)) {
            DataCollectionStatus status = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
            status = DataCollectionStatusUtils.updateTimeForCategoryChange(status,
                    getLongValueFromContext(PA_TIMESTAMP), Category.CONTACT_ATTRIBUTES);
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
                } else {
                    DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(), taskId);
                    if (dataFeedTask == null) {
                        continue;
                    } else if (dataFeedTask.getEntity().equals(BusinessEntity.Transaction.name())) {
                        this.newTransactionCount += importActionConfiguration.getImportCount();
                    }
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


    private void reachTransactionLimit() {
        Long dataCount;
        DataLimit dataLimit = getObjectFromContext(DATAQUOTA_LIMIT, DataLimit.class);
        Long transactionDataQuotaLimit = dataLimit.getTransactionDataQuotaLimit();
        DataCollectionStatus dataCollectionStatus = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        dataCount = this.newTransactionCount;
        if (dataCollectionStatus != null && dataCollectionStatus.getDetail() != null && dataCollectionStatus.getTransactionCount() != null) {
            DataCollectionStatusDetail detail = dataCollectionStatus.getDetail();
            dataCount = dataCount + detail.getTransactionCount();
        }

        log.info("stored Transaction data is " + dataCount);
        if (transactionDataQuotaLimit < dataCount)
            throw new IllegalStateException("the Transaction data quota limit is " + transactionDataQuotaLimit +
                    ", The data you uploaded has exceeded the limit.");
        log.info("stored data is " + dataCount + ", the Transaction data limit is " + transactionDataQuotaLimit);
    }

    private void createReportJson() {
        reportJson = JsonUtils.createObjectNode();
        putObjectInContext(ReportPurpose.PROCESS_ANALYZE_RECORDS_SUMMARY.getKey(), reportJson);
    }

    private void setupInactiveVersion() {
        Set<String> tableNamesForRetry = getTableNamesForPaRetry();
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
                    if (activeTableNameSet.contains(tableName.toLowerCase()) || tableNamesForRetry.contains(tableName)) {
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

    private void resetEntityMatchFlagsForRetry() {
        boolean completed = Boolean.TRUE.equals(getObjectFromContext(ENTITY_MATCH_COMPLETED, Boolean.class));
        if (!completed) {
            // need to rerun entity match steps
            removeObjectFromContext(NEW_ENTITY_MATCH_ENVS);
        }
    }

    private void setAttributeQuotaLimit() {
        AttributeLimit attrLimit = dataFeedProxy.getAttributeQuotaLimit(customerSpace.toString());
        if (attrLimit == null) {
            throw new IllegalArgumentException("Attribute quota limit can not be found.");
        }
        putObjectInContext(ATTRIBUTE_QUOTA_LIMIT, attrLimit);
    }

    private void setDataQuotaLimit() {
        DataLimit dataLimit = dataFeedProxy.getDataQuotaLimitMap(customerSpace);
        if (dataLimit == null) {
            throw new IllegalArgumentException("Data Quota Limit Can not find.");
        }
        putObjectInContext(DATAQUOTA_LIMIT, dataLimit);
    }

    private void setMigrationMode() {
        migrationMode = MigrationTrack.Status.STARTED.equals(metadataProxy.getMigrationStatus(customerSpace.toString()));
    }

    private void verifyActiveDataCollectionVersion() {
        if (!activeVersion.equals(metadataProxy.getMigrationActiveVersion(customerSpace.toString()))) {
            log.error("Current active version for tenant {} doesn't match the one in migration table", customerSpace);
            throw new IllegalStateException(String.format("Current active data collection version not match for %s", customerSpace));
        }
    }

    private void unlinkTables() {
        dataCollectionProxy.unlinkTables(customerSpace.toString(), activeVersion);
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
