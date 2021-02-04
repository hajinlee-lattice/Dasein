package com.latticeengines.cdl.workflow.steps.process;

import static com.latticeengines.domain.exposed.metadata.DataCollectionArtifact.Status.READY;
import static com.latticeengines.domain.exposed.metadata.DataCollectionArtifact.Status.STALE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.cdl.AttributeLimit;
import com.latticeengines.domain.exposed.cdl.ChoreographerContext;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.cdl.DataLimit;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchVersion;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionArtifact;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatusDetail;
import com.latticeengines.domain.exposed.metadata.MigrationTrack;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecutionJobType;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedImport;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.RedshiftDataUnit;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.AttrConfigLifeCycleChangeConfiguration;
import com.latticeengines.domain.exposed.pls.CleanupActionConfiguration;
import com.latticeengines.domain.exposed.pls.ImportActionConfiguration;
import com.latticeengines.domain.exposed.pls.LegacyDeleteByDateRangeActionConfiguration;
import com.latticeengines.domain.exposed.pls.LegacyDeleteByUploadActionConfiguration;
import com.latticeengines.domain.exposed.pls.RatingEngineActionConfiguration;
import com.latticeengines.domain.exposed.pls.SegmentActionConfiguration;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.ProcessAnalyzeWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.RedshiftExportConfig;
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
import com.latticeengines.proxy.exposed.matchapi.MatchProxy;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.redshiftdb.exposed.service.RedshiftPartitionService;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;
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

    @Inject
    private MatchProxy matchProxy;

    @Inject
    private DataUnitProxy dataUnitProxy;

    @Inject
    private BatonService batonService;

    @Inject
    private RedshiftPartitionService redshiftPartitionService;

    private CustomerSpace customerSpace;
    private DataCollection.Version activeVersion;
    private DataCollection.Version inactiveVersion;
    private ObjectNode reportJson;
    private ChoreographerContext grapherContext = new ChoreographerContext();
    private Long newTransactionCount = 0L;
    private boolean migrationMode;

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
        }

        String tenantName = CustomerSpace.shortenCustomerSpace(customerSpace.toString());
        log.info("tenantName is :" + tenantName);
        if (tenantName.equalsIgnoreCase("QA_Atlas_Auto_Refresh_2")) {
            String msg = "this warning message from tenant QA_Atlas_Auto_Refresh_2, just for testing.";
            addToListInContext(PROCESS_ANALYTICS_WARNING_KEY, msg, String.class);
        }

        if (!hasKeyInContext(FULL_REMATCH_PA)) {
            boolean fullRematch = Boolean.TRUE.equals(configuration.isFullRematch());
            putObjectInContext(FULL_REMATCH_PA, fullRematch);
        }

        if (!hasKeyInContext(NEW_RECORD_CUT_OFF_TIME)) {
            putLongValueInContext(NEW_RECORD_CUT_OFF_TIME, System.currentTimeMillis());
        }

        // lattice modules
        boolean isSSVITenant = BooleanUtils.isTrue(configuration.isSSVITenant());
        putObjectInContext(IS_SSVI_TENANT, isSSVITenant);
        boolean isCDLTenant = BooleanUtils.isTrue(configuration.isCDLTenant());
        putObjectInContext(IS_CDL_TENANT, isCDLTenant);

        // entityMatchVersion is using to bumpVersion when we Rematch
        // entityMatch tenant.
        EntityMatchVersion entityMatchVersion = matchProxy.getEntityMatchVersion(customerSpace.toString(),
                EntityMatchEnvironment.SERVING, false);
        log.info("entityMatchVersion is {}.", entityMatchVersion);
        putObjectInContext(ENTITY_MATCH_REMATCH_SERVING_VERSION, entityMatchVersion.getNextVersion());

        putObjectInContext(SKIP_PUBLISH_PA_TO_S3, configuration.isSkipPublishToS3());

        String evaluationDate = periodProxy.getEvaluationDate(customerSpace.toString());
        putStringValueInContext(CDL_EVALUATION_DATE, evaluationDate);
        if (!hasKeyInContext(PA_TIMESTAMP)) {
            putLongValueInContext(PA_TIMESTAMP, System.currentTimeMillis());
        } else if (StringUtils.isNotBlank(getStringValueFromContext(PA_TIMESTAMP))) {
            parsePATimestampString();
        }

        putObjectInContext(PA_SKIP_ENTITIES, configuration.getSkipEntities());

        setupDataCollectionStatus(evaluationDate);

        grapherContext.setEntityMatchEnabled(Boolean.TRUE.equals(configuration.isEntityMatchEnabled()));
        putObjectInContext(ENTITY_MATCH_ENABLED, Boolean.TRUE.equals(configuration.isEntityMatchEnabled()));

        updateDataFeed();

        createReportJson();
        setupInactiveVersion();
        setGrapherContext();
        resetEntityMatchFlagsForRetry();
        setAttributeQuotaLimit();
        setDataQuotaLimit();
        reachTransactionLimit();

        log.info("Evict attr repo cache for inactive version " + inactiveVersion);
        dataCollectionProxy.evictAttrRepoCache(customerSpace.toString(), inactiveVersion);
    }

    /*-
     * PA_TIMESTAMP should exist as string in context
     */
    private void parsePATimestampString() {
        String timestampStr = getStringValueFromContext(PA_TIMESTAMP);
        try {
            putLongValueInContext(PA_TIMESTAMP, Long.parseLong(timestampStr));
        } catch (NumberFormatException e) {
            String msg = String.format("PA timestamp string %s is not a valid number", timestampStr);
            throw new IllegalArgumentException(msg, e);
        }
    }

    private void updateDataFeed() {
        List<Long> actionIds = getImportActions().stream().map(Action::getPid).collect(Collectors.toList());
        DataFeedExecution execution = dataFeedProxy.startExecution(customerSpace.toString(),
                DataFeedExecutionJobType.PA, jobId, actionIds);
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
                activeVersion);
        dcStatus.setEvaluationDate(evaluationDate);
        dcStatus.setApsRollingPeriod(configuration.getApsRollingPeriod());
        log.info("StartProcessing step: dataCollection Status is " + JsonUtils.serialize(dcStatus));
        if (MapUtils.isEmpty(dcStatus.getDateMap())) {
            dcStatus = DataCollectionStatusUtils.initDateMap(dcStatus, getLongValueFromContext(PA_TIMESTAMP));
        }
        String newPartition = redshiftPartitionService.getDefaultPartition();
        dcStatus.setRedshiftPartition(newPartition);
        putObjectInContext(CDL_COLLECTION_STATUS, dcStatus);

        DataCollectionStatus inactiveStatus = dataCollectionProxy
                .getOrCreateDataCollectionStatus(customerSpace.toString(), inactiveVersion);
        if (inactiveStatus != null && configuration.getDataCloudBuildNumber() != null) {
            inactiveStatus.setDataCloudBuildNumber(configuration.getDataCloudBuildNumber());
            inactiveStatus.setRedshiftPartition(newPartition);
            dataCollectionProxy.saveOrUpdateDataCollectionStatus(customerSpace.toString(), inactiveStatus,
                    inactiveVersion);
        }

        syncRedshiftTablesToNewCluster(newPartition);
    }

    private void syncRedshiftTablesToNewCluster(String newPartition) {
        RedshiftService newRedshiftService = redshiftPartitionService.getSegmentUserService(newPartition);
        for (TableRoleInCollection servingStore : TableRoleInCollection.values()) {
            if (StringUtils.isNotBlank(servingStore.getDistKey())) {
                String tableName = //
                        dataCollectionProxy.getTableName(customerSpace.toString(), servingStore, activeVersion);
                if (StringUtils.isNotBlank(tableName)) {
                    RedshiftDataUnit dataUnit = (RedshiftDataUnit) dataUnitProxy
                            .getByNameAndType(customerSpace.toString(), tableName, DataUnit.StorageType.Redshift);
                    boolean newDataUnit = false;
                    if (dataUnit == null) {
                        dataUnit = new RedshiftDataUnit();
                        dataUnit.setName(tableName);
                        dataUnit.setTenant(customerSpace.getTenantId());
                        dataUnit.setRedshiftTable(tableName.toLowerCase());
                        newDataUnit = true;
                    }
                    if (!newPartition.equals(dataUnit.getClusterPartition())
                            || !newRedshiftService.hasTable(tableName)) {
                        log.info("Publishing redshift table {} to new partition {}", tableName, newPartition);
                        String distKey = servingStore.getDistKey();
                        List<String> sortKeys = new ArrayList<>(servingStore.getSortKeys());
                        String inputPath = metadataProxy.getAvroDir(configuration.getCustomerSpace().toString(),
                                tableName);
                        RedshiftExportConfig exportConfig = new RedshiftExportConfig();
                        exportConfig.setTableName(tableName);
                        exportConfig.setDistKey(distKey);
                        exportConfig.setSortKeys(sortKeys);
                        exportConfig.setInputPath(inputPath + "/*.avro");
                        exportConfig.setClusterPartition(newPartition);
                        exportConfig.setUpdateMode(false);
                        addToListInContext(TABLES_GOING_TO_REDSHIFT, exportConfig, RedshiftExportConfig.class);
                        if (!newPartition.equals(dataUnit.getClusterPartition())) {
                            dataUnit.setClusterPartition(newPartition);
                            try {
                                if (newDataUnit) {
                                    dataUnitProxy.create(customerSpace.toString(), dataUnit);
                                } else {
                                    dataUnitProxy.updateByNameAndType(customerSpace.toString(), dataUnit);
                                }
                            } catch (Exception e) {
                                log.warn("Failed to update redshift data unit", e);
                            }
                        }
                    }
                }
            }
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
        boolean[] ldcChanges = checkDataCloudChange();
        grapherContext.setDataCloudChanged(ldcChanges[0]);
        grapherContext.setDataCloudRefresh(ldcChanges[1]);
        grapherContext.setDataCloudNew(ldcChanges[2]);
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
        List<Action> softDeleteActions = getSoftOrHardDeleteActions(true, actions);
        List<Action> hardDeleteActions = getSoftOrHardDeleteActions(false, actions);
        putObjectInContext(SOFT_DELETE_ACTIONS, softDeleteActions);
        putObjectInContext(HARD_DELETE_ACTIONS, hardDeleteActions);
        setLegacyDeleteByUploadActions(actions);
        setLegacyDeleteByDateRangeActions(actions);

        grapherContext.setFullRematch(Boolean.TRUE.equals(getObjectFromContext(FULL_REMATCH_PA, Boolean.class)));
        grapherContext.setHasSoftDelete(CollectionUtils.isNotEmpty(softDeleteActions));
        grapherContext.setHasAccountBatchStore(hasAccountBatchStore());
        grapherContext.setHasContactBatchStore(hasContactBatchStore());
        grapherContext.setHasTransactionRawStore(hasTransactionRawStore());
        grapherContext.setSkipForceRebuildTxn(skipForceRebuildTxn());

        String tenantId = customerSpace.getTenantId();
        grapherContext.setAlwaysRebuildServingStores(shouldAlwaysRebuildServingStore(tenantId));

        putObjectInContext(CHOREOGRAPHER_CONTEXT_KEY, grapherContext);
    }

    /*-
     * TODO remove after all tenants are migrated off CustomerAccountId
     */
    private boolean skipForceRebuildTxn() {
        try {
            Camille c = CamilleEnvironment.getCamille();
            Path path = PathBuilder.buildSkipForceTxnRebuildListPath(CamilleEnvironment.getPodId());
            if (!c.exists(path)) {
                return false;
            }
            String tenantsStr = c.get(path).getData();
            log.info("Tenant list to skip force rebuild transaction = {}", tenantsStr);
            if (StringUtils.isBlank(tenantsStr)) {
                return false;
            }

            String tenantId = CustomerSpace.shortenCustomerSpace(customerSpace.getTenantId());
            String[] tenants = tenantsStr.split(",");
            boolean skip = Arrays.stream(tenants) //
                    .filter(StringUtils::isNotBlank) //
                    .map(CustomerSpace::shortenCustomerSpace) //
                    .filter(StringUtils::isNotBlank) //
                    .anyMatch(tenantId::equals);
            if (skip) {
                log.info("Skip force rebuilding txn for tenant {} since it's in list", tenantId);
            }
            return skip;
        } catch (Exception e) {
            log.warn("Fail to check skip force rebuild transaction (for CustomerAccountId) tenant list", e);
            return false;
        }
    }

    Set<BusinessEntity> getEntitiesShouldRebuildByActions() {
        return RebuildEntitiesProvider.getRebuildEntities(this);
    }

    private boolean[] checkDataCloudChange() {
        boolean changed = false;
        boolean newLdc = false;
        boolean weeklyRefresh = false;
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
            newLdc = status == null || status.getDataCloudBuildNumber() == null
                    || DataCollectionStatusDetail.NOT_SET.equals(status.getDataCloudBuildNumber());
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
                putObjectInContext(HAS_DATA_CLOUD_MAJOR_CHANGE, Boolean.TRUE);
            } else if (StringUtils.compare(currentVersion == null ? null : currentVersion.getRefreshVersionVersion(),
                    statusVersion.getRefreshVersionVersion()) != 0) {
                createSystemAction(ActionType.INTENT_CHANGE, ActionType.INTENT_CHANGE.getDisplayName());
                DataCollectionStatus status = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
                status = DataCollectionStatusUtils.updateTimeForCategoryChange(status,
                        getLongValueFromContext(PA_TIMESTAMP), Category.INTENT);
                putObjectInContext(CDL_COLLECTION_STATUS, status);
                putObjectInContext(HAS_DATA_CLOUD_MINOR_CHANGE, Boolean.TRUE);
                weeklyRefresh = true;
            }
        }

        return new boolean[] { changed, weeklyRefresh, newLdc };
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

    boolean hasContactBatchStore() {
        String accountTableName = dataCollectionProxy.getTableName(customerSpace.toString(), //
                TableRoleInCollection.ConsolidatedContact, activeVersion);
        boolean hasBatchStore = StringUtils.isNotBlank(accountTableName);
        log.info("Account batch store exist=" + hasBatchStore);
        return hasBatchStore;
    }

    boolean hasTransactionRawStore() {
        String accountTableName = dataCollectionProxy.getTableName(customerSpace.toString(), //
                TableRoleInCollection.ConsolidatedRawTransaction, activeVersion);
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

    protected List<String> getActionImpactedEngineIds(List<Action> actions) {
        List<String> engineIds = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(actions)) {
            for (Action action : actions) {
                if (ActionType.getRatingRelatedTypes().contains(action.getType()) && //
                        action.getActionConfiguration() instanceof RatingEngineActionConfiguration) {
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

    private List<Action> getSoftOrHardDeleteActions(boolean softDelete, List<Action> actions) {
        ActionType actionType = softDelete ? ActionType.SOFT_DELETE : ActionType.HARD_DELETE;
        if (CollectionUtils.isNotEmpty(actions)) {
            return actions.stream().filter(action -> actionType.equals(action.getType())).collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    private void setLegacyDeleteByUploadActions(List<Action> actions) {
        Set<Action> accountLegacyDeleteByUploadActions = new HashSet<>();
        Set<Action> contactLegacyDeleteByUploadActions = new HashSet<>();
        Map<CleanupOperationType, Set<Action>> transactionLegacyDeleteByUploadActions = new HashMap<>();
        if (CollectionUtils.isEmpty(actions)) {
            return;
        }
        for (Action action : actions) {
            if (!ActionType.LEGACY_DELETE_UPLOAD.equals(action.getType())) {
                continue;
            }
            LegacyDeleteByUploadActionConfiguration config = (LegacyDeleteByUploadActionConfiguration) action
                    .getActionConfiguration();
            if (BusinessEntity.Account.equals(config.getEntity())) {
                accountLegacyDeleteByUploadActions.add(action);
                continue;
            }
            if (BusinessEntity.Contact.equals(config.getEntity())) {
                contactLegacyDeleteByUploadActions.add(action);
                continue;
            }
            if (BusinessEntity.Transaction.equals(config.getEntity())) {
                transactionLegacyDeleteByUploadActions.putIfAbsent(config.getCleanupOperationType(), new HashSet<>());
                Set<Action> actionSet = transactionLegacyDeleteByUploadActions.get(config.getCleanupOperationType());
                actionSet.add(action);
                transactionLegacyDeleteByUploadActions.put(config.getCleanupOperationType(), actionSet);
            }
        }
        log.info("transactionLegacyDeleteByUploadActions is {}.",
                JsonUtils.serialize(transactionLegacyDeleteByUploadActions));
        log.info("accountLegacyDeleteByUploadActions is {}.", accountLegacyDeleteByUploadActions);
        log.info("contactLegacyDeleteByUploadActions is {}.", contactLegacyDeleteByUploadActions);
        putObjectInContext(ACCOUNT_LEGACY_DELTE_BYUOLOAD_ACTIONS, accountLegacyDeleteByUploadActions);
        putObjectInContext(CONTACT_LEGACY_DELTE_BYUOLOAD_ACTIONS, contactLegacyDeleteByUploadActions);
        putObjectInContext(TRANSACTION_LEGACY_DELTE_BYUOLOAD_ACTIONS, transactionLegacyDeleteByUploadActions);
    }

    private void setLegacyDeleteByDateRangeActions(List<Action> actions) {
        Map<BusinessEntity, Set<Action>> legacyDeleteByDateRangeActions = new HashMap<>();
        if (CollectionUtils.isNotEmpty(actions)) {
            for (Action action : actions) {
                if (ActionType.LEGACY_DELETE_DATERANGE.equals(action.getType())) {
                    LegacyDeleteByDateRangeActionConfiguration config = (LegacyDeleteByDateRangeActionConfiguration) action
                            .getActionConfiguration();
                    Set<Action> actionSet = legacyDeleteByDateRangeActions.get(config.getEntity());
                    if (actionSet == null) {
                        actionSet = new HashSet<>();
                    }
                    actionSet.add(action);
                    legacyDeleteByDateRangeActions.put(config.getEntity(), actionSet);
                }
            }
        }
        log.info("legacyDeleteByDateRangeActions is {}.", JsonUtils.serialize(legacyDeleteByDateRangeActions));
        putObjectInContext(LEGACY_DELETE_BYDATERANGE_ACTIONS, legacyDeleteByDateRangeActions);
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

    private List<Action> getTransactionDeleteActions() {
        List<Action> actionList = getActions();
        List<Action> deleteActions = actionList.stream()
                .filter(action -> ActionType.CDL_OPERATION_WORKFLOW.equals(action.getType())
                        || ActionType.DATA_REPLACE.equals(action.getType()))
                .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(deleteActions)) {
            return deleteActions;
        }
        List<Action> deleteTransactionActions = new ArrayList<>();
        for (Action action : deleteActions) {
            if (!(action.getActionConfiguration() instanceof CleanupActionConfiguration)) {
                continue;
            }
            CleanupActionConfiguration cleanupActionConfiguration = (CleanupActionConfiguration) action
                    .getActionConfiguration();
            if (cleanupActionConfiguration.getImpactEntities().contains(BusinessEntity.Transaction)) {
                deleteTransactionActions.add(action);
            }
        }
        return deleteTransactionActions;
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
            Map<String, String> tableTemplateMap = getMapObjectFromContext(CONSOLIDATE_INPUT_TEMPLATES, String.class,
                    String.class);
            if (tableTemplateMap == null) {
                tableTemplateMap = new HashMap<>();
            }
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

                if (configuration.isEntityMatchEnabled()) {
                    DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(),
                            importActionConfiguration.getDataFeedTaskId());
                    String templateName = dataFeedProxy.getTemplateName(customerSpace.toString(),
                            dataFeedTask.getUniqueId());
                    associateTemplates(tables, templateName, tableTemplateMap);
                }
            }
            putObjectInContext(CONSOLIDATE_INPUT_TEMPLATES, tableTemplateMap);
        }
        if (configuration.isEntityMatchEnabled()) {
            setTemplatesInOrder();
        }
    }

    private void setTemplatesInOrder() {
        List<String> templatesForAccount = dataFeedProxy.getTemplatesBySystemPriority(customerSpace.toString(),
                BusinessEntity.Account.name(), false);
        List<String> templatesForContact = dataFeedProxy.getTemplatesBySystemPriority(customerSpace.toString(),
                BusinessEntity.Contact.name(), false);
        log.info("Account templates in order=" + String.join(",", templatesForAccount));
        log.info("Contact templates in order=" + String.join(",", templatesForContact));
        Map<BusinessEntity, List<String>> templatesInOrder = new HashMap<>();
        templatesInOrder.put(BusinessEntity.Account, templatesForAccount);
        templatesInOrder.put(BusinessEntity.Contact, templatesForContact);
        putObjectInContext(CONSOLIDATE_TEMPLATES_IN_ORDER, templatesInOrder);
    }

    private void associateTemplates(List<String> tables, String template, Map<String, String> tableTemplateMap) {
        for (int i = 0; i < tables.size(); i++) {
            tableTemplateMap.put(tables.get(i), template);
        }
    }

    /**
     * check transaction limit has two step: this is first step, if there is no
     * delete transaction action in this PA we will check transaction quota limit.
     * otherwise, we will check quota in merge step
     */
    private void reachTransactionLimit() {
        List<Action> deleteTransactionActions = getTransactionDeleteActions();
        if (CollectionUtils.isEmpty(deleteTransactionActions)) {
            Long dataCount = this.newTransactionCount;
            DataLimit dataLimit = getObjectFromContext(DATAQUOTA_LIMIT, DataLimit.class);
            Long transactionDataQuotaLimit = dataLimit.getTransactionDataQuotaLimit();
            DataCollectionStatus dataCollectionStatus = getObjectFromContext(CDL_COLLECTION_STATUS,
                    DataCollectionStatus.class);
            if (dataCollectionStatus != null && dataCollectionStatus.getDetail() != null
                    && dataCollectionStatus.getTransactionCount() != null) {
                DataCollectionStatusDetail detail = dataCollectionStatus.getDetail();
                dataCount = dataCount + detail.getTransactionCount();
            }

            log.info("stored Transaction data is {}.", dataCount);
            if (transactionDataQuotaLimit < dataCount) {
                throw new IllegalStateException("the Transaction data quota limit is " + transactionDataQuotaLimit
                        + ", The data you uploaded has exceeded the limit.");
            }
            log.info("stored data is {}, the Transaction data limit is {}.", dataCount, transactionDataQuotaLimit);
        }
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
                    if (activeTableNameSet.contains(tableName.toLowerCase())
                            || tableNamesForRetry.contains(tableName)) {
                        log.info("Unlink table " + tableName + " as " + role + " in " + inactiveVersion);
                        dataCollectionProxy.unlinkTable(customerSpace.toString(), tableName, role, inactiveVersion);
                    } else {
                        log.info("Removing table " + tableName + " as " + role + " in " + inactiveVersion);
                        metadataProxy.deleteTable(customerSpace.toString(), tableName);
                    }
                }
            }
        }

        new Thread(() -> {
            boolean statsUpdated = Boolean.TRUE.equals(getObjectFromContext(STATS_UPDATED, Boolean.class));
            if (!statsUpdated) {
                log.info("Removing stats in " + inactiveVersion);
                dataCollectionProxy.removeStats(customerSpace.toString(), inactiveVersion);
            } else {
                log.info("Stats has been updated in previous job, keep inactive (version = {}) stats", inactiveVersion);
            }

            List<DataCollectionArtifact> artifacts =
                    dataCollectionProxy.getDataCollectionArtifacts(customerSpace.toString(), READY, inactiveVersion);
            if (CollectionUtils.isNotEmpty(artifacts)) {
                for (DataCollectionArtifact artifact: artifacts) {
                    artifact.setStatus(STALE);
                    dataCollectionProxy.updateDataCollectionArtifact(customerSpace.toString(), artifact);
                    log.info("Update {} artifact in version {} to STALE", artifact.getName(), artifact.getVersion());
                }
            }
        }).start();
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
        MigrationTrack.Status status = metadataProxy.getMigrationStatus(customerSpace.toString());
        log.info("Tenant's migration status is {}.", status);
        migrationMode = MigrationTrack.Status.STARTED.equals(status);
        log.info("Migration mode is {}", migrationMode ? "on" : "off");
    }

    protected boolean shouldAlwaysRebuildServingStore(String tenantId) {
        return batonService.shouldExcludeDataCloudAttrs(tenantId);
    }

    private void verifyActiveDataCollectionVersion() {
        if (!activeVersion.equals(metadataProxy.getMigrationActiveVersion(customerSpace.toString()))) {
            log.error("Current active version for tenant {} doesn't match the one in migration table", customerSpace);
            throw new IllegalStateException(
                    String.format("Current active data collection version not match for %s", customerSpace));
        }
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
