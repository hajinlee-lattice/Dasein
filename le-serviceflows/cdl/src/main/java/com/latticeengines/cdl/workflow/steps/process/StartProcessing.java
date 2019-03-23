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
import com.google.common.collect.Sets;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ChoreographerContext;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.datacloud.match.entity.BumpVersionRequest;
import com.latticeengines.domain.exposed.datacloud.match.entity.BumpVersionResponse;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
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
import com.latticeengines.domain.exposed.pls.SegmentActionConfiguration;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.ProcessAnalyzeWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessStepConfiguration;
import com.latticeengines.domain.exposed.util.DataCollectionStatusUtils;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.domain.exposed.workflow.BaseWrapperStepConfiguration;
import com.latticeengines.domain.exposed.workflow.BaseWrapperStepConfiguration.Phase;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.proxy.exposed.cdl.ActionProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.cdl.PeriodProxy;
import com.latticeengines.proxy.exposed.matchapi.MatchProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
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
    private MatchProxy matchProxy;

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
    private ChoreographerContext grapherContext = new ChoreographerContext();;

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
        Set<String> renewableKeys = Sets.newHashSet( //
                NEW_ENTITY_MATCH_VERSION, //
                ACCOUNT_DIFF_TABLE_NAME, //
                ACCOUNT_MASTER_TABLE_NAME, //
                FULL_ACCOUNT_TABLE_NAME, //
                ACCOUNT_FEATURE_TABLE_NAME, //
                ACCOUNT_PROFILE_TABLE_NAME, //
                ACCOUNT_SERVING_TABLE_NAME, //
                ACCOUNT_STATS_TABLE_NAME
        );
        clearExecutionContext(renewableKeys);
        customerSpace = configuration.getCustomerSpace();
        addActionAssociateTables();
        determineVersions();

        String evaluationDate = periodProxy.getEvaluationDate(customerSpace.toString());
        putStringValueInContext(CDL_EVALUATION_DATE, evaluationDate);
        putLongValueInContext(PA_TIMESTAMP, System.currentTimeMillis());
        putObjectInContext(PA_SKIP_ENTITIES, configuration.getSkipEntities());

        setupDataCollectionStatus(evaluationDate);

        updateDataFeed();

        createReportJson();
        setupInactiveVersion();
        setGrapherContext();
        bumpEntityMatchVersion();
        // clearPhaseForRetry();
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
        dcStatus.setDataCloudBuildNumber(configuration.getDataCloudBuildNumber());
        putObjectInContext(CDL_COLLECTION_STATUS, dcStatus);

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
        List<Action> businessCalenderChangedActions = getBusinessCalendarChangedActions(actions);
        grapherContext.setBusinessCalenderChanged(CollectionUtils.isNotEmpty(businessCalenderChangedActions));

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
                            || DataCollectionStatus.NOT_SET.equals(status.getDataCloudBuildNumber())
                            || !status.getDataCloudBuildNumber().equals(currentBuildNumber))
                    && hasAccountBatchStore()) {
                statusBuildNumber = DataCollectionStatus.NOT_SET.equals(status.getDataCloudBuildNumber()) ? null
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
        action.setActionInitiator(WorkflowUser.DEFAULT_USER.name());
        action.setOwnerId(configuration.getOwnerId());
        action = actionProxy.createAction(customerSpace.getTenantId(), action);
        List<Long> systemActionIds = getListObjectFromContext(SYSTEM_ACTION_IDS, Long.class);
        if (CollectionUtils.isEmpty(systemActionIds)) {
            systemActionIds = new ArrayList<>();
        }
        systemActionIds.add(action.getPid());
        putObjectInContext(SYSTEM_ACTION_IDS, systemActionIds);
        log.info(String.format("System actions are: %s", JsonUtils.serialize(systemActionIds)));
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

    private List<Action> getBusinessCalendarChangedActions(List<Action> actions) {
        return actions.stream().filter(action -> action.getType() == ActionType.BUSINESS_CALENDAR_CHANGE)
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

    private void createReportJson() {
        reportJson = JsonUtils.createObjectNode();
        putObjectInContext(ReportPurpose.PROCESS_ANALYZE_RECORDS_SUMMARY.getKey(), reportJson);
    }

    private void setupInactiveVersion() {
        Set<String> tableKeysForRetry = Sets.newHashSet( //
                ACCOUNT_DIFF_TABLE_NAME, //
                ACCOUNT_MASTER_TABLE_NAME, //
                FULL_ACCOUNT_TABLE_NAME, //
                ACCOUNT_FEATURE_TABLE_NAME, //
                ACCOUNT_PROFILE_TABLE_NAME, //
                ACCOUNT_SERVING_TABLE_NAME, //
                ACCOUNT_STATS_TABLE_NAME
        );
        Set<String> tableNamesForRetry = tableKeysForRetry.stream() //
                .map(this::getStringValueFromContext) //
                .filter(StringUtils::isNotBlank) //
                .collect(Collectors.toSet());
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

    /*
     * bump up entity match version
     */
    private void bumpEntityMatchVersion() {
        Boolean alreadyBumpedUp = getObjectFromContext(NEW_ENTITY_MATCH_VERSION, Boolean.class);
        if (Boolean.TRUE.equals(alreadyBumpedUp)) {
            log.info("Already handled entity match version bumping up in previous PA.");
            return;
        }

        if (!configuration.isEntityMatchEnabled()) {
            log.debug("Entity match is not enabled, not bumping up version");
            return;
        }
        List<EntityMatchEnvironment> environments = getEnvironmentsToBumpVersion();
        if (CollectionUtils.isNotEmpty(environments)) {
            BumpVersionRequest request = new BumpVersionRequest();
            request.setTenant(new Tenant(customerSpace.toString()));
            request.setEnvironments(environments);
            log.info("Bump up entity match version for environments = {}", environments);
            BumpVersionResponse response = matchProxy.bumpVersion(request);
            log.info("Current entity match versions = {}", response.getVersions());
        } else {
            log.debug("No entity match environment requires bump up version");
        }
        putObjectInContext(NEW_ENTITY_MATCH_VERSION, Boolean.TRUE);
    }

    /*
     * Get all entity match environments that requires bump up version. Currently
     * only consider Account import & rebuild.
     */
    @VisibleForTesting
    List<EntityMatchEnvironment> getEnvironmentsToBumpVersion() {
        Set<EntityMatchEnvironment> environments = new HashSet<>();
        Map<BusinessEntity, List> entityImportsMap = getMapObjectFromContext(CONSOLIDATE_INPUT_IMPORTS,
                BusinessEntity.class, List.class);
        if (MapUtils.isNotEmpty(entityImportsMap) && entityImportsMap.containsKey(BusinessEntity.Account)) {
            // if there is account import, bump up staging version
            environments.add(EntityMatchEnvironment.STAGING);
        }
        if (CollectionUtils.isNotEmpty(configuration.getRebuildEntities())
                && configuration.getRebuildEntities().contains(BusinessEntity.Account)) {
            // when rebuilding for account, bump up both staging & serving version
            environments.add(EntityMatchEnvironment.STAGING);
            environments.add(EntityMatchEnvironment.SERVING);
        }
        return new ArrayList<>(environments);
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
