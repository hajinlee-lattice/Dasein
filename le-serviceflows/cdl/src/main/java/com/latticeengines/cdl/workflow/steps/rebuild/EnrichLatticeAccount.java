package com.latticeengines.cdl.workflow.steps.rebuild;

import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ConsolidatedAccount;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.LatticeAccount;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.cdl.workflow.steps.BaseProcessAnalyzeSparkStep;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.AvroInputBuffer;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.datacloud.match.RefreshFrequency;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.MergeImportsConfig;
import com.latticeengines.domain.exposed.spark.cdl.MergeLatticeAccountConfig;
import com.latticeengines.domain.exposed.spark.cdl.TruncateLatticeAccountConfig;
import com.latticeengines.domain.exposed.spark.common.ChangeListConfig;
import com.latticeengines.domain.exposed.spark.common.ChangeListConstants;
import com.latticeengines.domain.exposed.spark.common.CopyConfig;
import com.latticeengines.domain.exposed.spark.common.FilterByJoinConfig;
import com.latticeengines.domain.exposed.spark.common.FilterChangelistConfig;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.proxy.exposed.cdl.SubscriptionProxy;
import com.latticeengines.proxy.exposed.cdl.TimeLineProxy;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.serviceflows.workflow.match.BulkMatchService;
import com.latticeengines.spark.exposed.job.cdl.MergeImportsJob;
import com.latticeengines.spark.exposed.job.cdl.MergeLatticeAccount;
import com.latticeengines.spark.exposed.job.cdl.TruncateLatticeAccount;
import com.latticeengines.spark.exposed.job.common.CopyJob;
import com.latticeengines.spark.exposed.job.common.CreateChangeListJob;
import com.latticeengines.spark.exposed.job.common.FilterByJoinJob;
import com.latticeengines.spark.exposed.job.common.FilterChangelistJob;

@Lazy
@Component(EnrichLatticeAccount.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public class EnrichLatticeAccount extends BaseProcessAnalyzeSparkStep<ProcessAccountStepConfiguration> {

    static final String BEAN_NAME = "enrichLatticeAccount";

    private static final Logger log = LoggerFactory.getLogger(EnrichLatticeAccount.class);

    @Inject
    private ColumnMetadataProxy columnMetadataProxy;

    @Inject
    private BulkMatchService bulkMatchService;

    @Inject
    private ServingStoreProxy servingStoreProxy;

    @Inject
    private BatonService batonService;

    @Inject
    private TimeLineProxy timeLineProxy;

    @Inject
    private SubscriptionProxy subscriptionProxy;

    private Table oldLatticeAccountTable = null;
    private Table newLatticeAccountTable = null;
    private Table latticeAccountChangeListTable = null;
    private Table accountBatchStore = null;
    private Table accountChangeList = null;
    private String joinKey;
    private String ldcTablePrefix;
    private DataUnit accountChangeListDU = null;
    private DataUnit oldLatticeAccountDU = null;
    private HdfsDataUnit deleted = null;
    private HdfsDataUnit changed = null;

    private List<String> fetchAttrs;
    private List<String> attrs2Remove;
    private List<String> attrs2Add;
    private List<String> attrs2Update;
    private boolean fetchAll;
    private boolean rebuildDownstream;
    private List<DataUnit> changeLists = new ArrayList<>();
    private ColumnSelection allActiveColumns;

    // Default firmographic attributes required by Intent email alert
    private static final Set<String> DEFAULT_FIRMOGRAPHIC_ATTRIBUTES = ImmutableSet.of("GLOBAL_ULTIMATE_DUNS_NUMBER",
            "DOMESTIC_ULTIMATE_DUNS_NUMBER", "LDC_DUNS", "LDC_Domain", "LDC_Name", "LDC_City", "STATE_PROVINCE_ABBR",
            "LDC_Country", "LE_IS_PRIMARY_DOMAIN", "LDC_PrimaryIndustry", "LE_REVENUE_RANGE");

    @Override
    public void execute() {
        bootstrap();

        List<Table> tablesInCtx = getTableSummariesFromCtxKeys(customerSpaceStr.toLowerCase(),
                Arrays.asList(LATTICE_ACCOUNT_TABLE_NAME, LATTICE_ACCOUNT_CHANGELIST_TABLE_NAME));
        boolean shortCutMode = tablesInCtx.stream().noneMatch(Objects::isNull);
        if (shortCutMode) {
            log.info("Found LatticeAccount and LatticeAccount changelist in context, go through short-cut mode.");
            String latticeAccountTableName = tablesInCtx.get(0).getName();
            dataCollectionProxy.upsertTable(customerSpace.toString(), latticeAccountTableName, LatticeAccount,
                    inactive);
        } else {
            if (shouldDoNothing()) {
                linkInactiveTable(LatticeAccount);
            } else {
                preExecution();
                buildLatticeAccount();
                postExecution();
            }
        }
    }

    private boolean shouldDoNothing() {
        // some special tenants do not want to use LDC attributes
        boolean noLDC = batonService.shouldExcludeDataCloudAttrs(customerSpace.getTenantId());
        if (noLDC) {
            return true;
        } else {
            boolean hasAccountChange = isChanged(ConsolidatedAccount, ACCOUNT_CHANGELIST_TABLE_NAME);
            oldLatticeAccountTable = attemptGetTableRole(LatticeAccount, false);
            if (oldLatticeAccountTable != null) {
                oldLatticeAccountDU = toDataUnit(oldLatticeAccountTable, "OldLatticeAccount");
            }
            fetchAttrs = getFetchAttrs();
            fetchAll = shouldFetchAll();
            rebuildDownstream = shouldRebuildDownstream();
            boolean hasAttrChanges = hasAttrChanges();
            boolean doNothing = !(hasAccountChange || fetchAll || rebuildDownstream || hasAttrChanges);
            log.info("hasAccountChange={}, fetchAll={}, rebuildDownstream={}, hasAttrChanges={}: doNothing={}",
                    hasAccountChange, fetchAll, rebuildDownstream, hasAttrChanges, doNothing);
            return doNothing;
        }
    }

    private boolean shouldRebuildDownstream() {
        return oldLatticeAccountTable == null || Boolean.TRUE.equals(configuration.getRebuild());
    }

    private boolean shouldFetchAll() {
        // should ignore account change list, just fetch for all accounts
        // note that this does not mean downstream has to rebuild
        // examples are: activating new attributes, data cloud weekly refresh
        boolean hasDataCloudMajorChange = //
                Boolean.TRUE.equals(getObjectFromContext(HAS_DATA_CLOUD_MAJOR_CHANGE, Boolean.class));
        accountChangeList = getTableSummaryFromKey(customerSpace.toString(), ACCOUNT_CHANGELIST_TABLE_NAME);
        boolean missAccountChangeList = isChanged(ConsolidatedAccount) && (accountChangeList == null);
        boolean missLatticeAccountTable = (oldLatticeAccountTable == null);
        boolean enforceRebuild = Boolean.TRUE.equals(configuration.getRebuild());
        boolean shouldFetchAll = //
                hasDataCloudMajorChange || missAccountChangeList || enforceRebuild || missLatticeAccountTable;
        log.info("hasDataCloudMajorChange={}, missAccountChangeList={}, enforceRebuild={}, missLatticeAccountTable={}: "
                + //
                "shouldFetchAll={}", //
                hasDataCloudMajorChange, missAccountChangeList, enforceRebuild, missLatticeAccountTable,
                shouldFetchAll);
        return shouldFetchAll;
    }

    private boolean hasAttrChanges() {
        attrs2Remove = findAttrs2Remove();
        attrs2Add = findAttrs2Add();
        attrs2Update = findAttrs2Update();
        return CollectionUtils.isNotEmpty(attrs2Remove) || CollectionUtils.isNotEmpty(attrs2Add)
                || CollectionUtils.isNotEmpty(attrs2Update);
    }

    private void preExecution() {
        joinKey = InterfaceName.AccountId.name();
        log.info("joinKey {}", joinKey);
        ldcTablePrefix = LatticeAccount.name();
        accountBatchStore = attemptGetTableRole(BusinessEntity.Account.getBatchStore(), true);
        allActiveColumns = selectActiveDataCloudAttrs();
    }

    protected void postExecution() {
        if (rebuildDownstream) {
            markRebuildFlag();
        } else {
            saveLatticeAccountChangelist();
        }

        saveLatticeAccount();
    }

    private void buildLatticeAccount() {
        if (fetchAll) {
            log.info("EnrichLatticeAccount, rebuild LatticeAccount by fetching all accounts");
            rebuildLatticeAccount();
        } else {
            log.info("EnrichLatticeAccount, update LatticeAccount based on Account changelist");
            updateLatticeAccount();
        }
    }

    private void rebuildLatticeAccount() {
        // Enrich with all accounts in account batch
        log.info("account batch store name is {}, path is {} ", accountBatchStore.getName(),
                accountBatchStore.getExtracts().get(0).getPath());
        HdfsDataUnit accountTable = accountBatchStore.toHdfsDataUnit("AccountBatchStore");
        // Filter account table to get only <AccountId, LatticeAccountId>
        HdfsDataUnit selected = select(accountTable);

        // fetch by bulk match
        HdfsDataUnit fetched = fetch(selected, allActiveColumns);

        // convert to parquet
        CopyConfig copyConfig = new CopyConfig();
        copyConfig.setInput(Collections.singletonList(fetched));
        copyConfig.setSpecialTarget(0, DataUnit.DataFormat.PARQUET);
        SparkJobResult result = runSparkJob(CopyJob.class, copyConfig);
        HdfsDataUnit newLatticeAccount = result.getTargets().get(0);

        // Only create LatticeAccount changelist when rebuildDownstream is set to false,
        // which means LatticeAccount changelist will be used in downstream
        if (!rebuildDownstream) {
            try {
                createLatticeAccountChangelist(newLatticeAccount);
            } catch (Exception e) {
                log.warn("Failed to generate change list for full LatticeAccount table, " + //
                        "force downstream to rebuild: {}", e.getMessage());
                rebuildDownstream = true;
                latticeAccountChangeListTable = null;
            }
        }

        // save LatticeAccount table
        String latticeAccountTableName = NamingUtils.timestamp(ldcTablePrefix);
        newLatticeAccountTable = toTable(latticeAccountTableName, newLatticeAccount);
    }

    // Categorize all possible changes in LatticeAccount table, truncate or fetch
    // LDC accordingly to update LatticeAccount table and generate changelist.
    // The principle is to minimize fetch and changelist generation steps
    // as they can be costly
    private void updateLatticeAccount() {
        Preconditions.checkNotNull(oldLatticeAccountTable, "Old LatticeAccount table must exist.");

        if (accountChangeList != null) {
            accountChangeListDU = toDataUnit(accountChangeList, "AccountChangeList");
        }

        // First, filter Account changelist to get deleted accounts
        // and accounts have changed LatticeAccountId
        filterChangelist();

        // When there are deleted accounts or removed attributes,
        // truncate the LatticeAccount table directly and generate changelist
        // accordingly
        if (CollectionUtils.isNotEmpty(attrs2Remove) || (deleted != null && deleted.getCount() > 0)) {
            truncateLatticeAccount(deleted, attrs2Remove);
        }

        // When there are attributes added or updated, fetch vertically
        if (CollectionUtils.isNotEmpty(attrs2Add) || CollectionUtils.isNotEmpty(attrs2Update)) {
            verticalFetch(attrs2Add, attrs2Update);
        }

        // When there are accounts have changed LatticeAccountIds, fetch horizontally
        if ((changed != null) && (changed.getCount() > 0)) {
            horizontalFetch(changed);
        }

        // Concatenate all possible changelist generated above to
        // get final LatticeAccount changelist
        if (changeLists.size() > 0) {
            mergeChangeList();
            // save LatticeAccount table
            String latticeAccountTableName = NamingUtils.timestamp(ldcTablePrefix);
            newLatticeAccountTable = toTable(latticeAccountTableName, (HdfsDataUnit) oldLatticeAccountDU);
        } else {
            log.info("No meaningful changes, relink LatticeAccount table.");
            linkInactiveTable(LatticeAccount);
        }

    }

    private HdfsDataUnit select(HdfsDataUnit input) {
        log.info("EnrichLatticeAccount, select <AccountId, LatticeAccountId> pairs from Account batch");

        CopyConfig config = new CopyConfig();
        List<DataUnit> inputs = new LinkedList<>();
        inputs.add(input);
        config.setInput(inputs);
        config.setSelectAttrs(Arrays.asList( //
                InterfaceName.AccountId.name(), //
                InterfaceName.LatticeAccountId.name() //
        ));
        config.setFillTimestamps(true);
        SparkJobResult result = runSparkJob(CopyJob.class, config);

        return result.getTargets().get(0);
    }

    private void createLatticeAccountChangelist(HdfsDataUnit newLatticeAccountDU) {
        log.info("EnrichLatticeAccount, create LatticeAccount changelist for rebuild");
        List<DataUnit> inputs = new LinkedList<>();
        inputs.add(newLatticeAccountDU); // toTable: new table
        if (oldLatticeAccountDU != null) {
            inputs.add(oldLatticeAccountDU); // fromTable: original table
        }
        ChangeListConfig config = new ChangeListConfig();
        config.setInput(inputs);
        config.setJoinKey(joinKey);
        config.setExclusionColumns(Arrays.asList( //
                InterfaceName.CDLCreatedTime.name(), //
                InterfaceName.CDLUpdatedTime.name(), //
                joinKey, //
                InterfaceName.LatticeAccountId.name() //
        ));
        setPartitionMultiplier(4);
        SparkJobResult result = runSparkJob(CreateChangeListJob.class, config);
        setPartitionMultiplier(1);

        // Save change list table
        String tableName = NamingUtils.timestamp("LatticeAccountChangeList");
        latticeAccountChangeListTable = toTable(tableName, result.getTargets().get(0));
    }

    // Filter Account changelist to get deleted accounts
    // and accounts have changed LatticeAccountId
    private void filterChangelist() {
        if (accountChangeListDU != null) {
            log.info(
                    "EnrichLatticeAccount, filter Account changelist to get accounts deleted or LatticeAccountId changed");

            FilterChangelistConfig config = new FilterChangelistConfig();
            List<DataUnit> inputs = new LinkedList<>();
            inputs.add(accountChangeListDU);
            config.setInput(inputs);
            config.setKey(InterfaceName.AccountId.name());
            config.setColumnId(InterfaceName.LatticeAccountId.name());
            config.setSelectColumns(
                    Arrays.asList(InterfaceName.AccountId.name(), InterfaceName.LatticeAccountId.name()));

            SparkJobResult result = runSparkJob(FilterChangelistJob.class, config);
            changed = result.getTargets().get(0);
            deleted = result.getTargets().get(1);
            log.info("There are {} accounts have changed LatticeAccountId", changed.getCount());
            log.info("There are {} accounts deleted", deleted.getCount());
        }
    }

    private void truncateLatticeAccount(HdfsDataUnit deletion, List<String> attrs2Remove) {
        log.info("EnrichLatticeAccount, truncate LatticeAccount table directly");

        TruncateLatticeAccountConfig config = new TruncateLatticeAccountConfig();
        config.setRemoveAttrs(attrs2Remove);
        config.setIgnoreAttrs(Arrays.asList( //
                InterfaceName.CDLCreatedTime.name(), //
                InterfaceName.CDLUpdatedTime.name(), //
                joinKey, //
                InterfaceName.LatticeAccountId.name() //
        ));
        List<DataUnit> inputs = new LinkedList<>();
        inputs.add(oldLatticeAccountDU); // source table: current LatticeAccount table
        if (deletion != null && deletion.getCount() > 0) {
            inputs.add(deletion); // delete changelist
        }
        config.setInput(inputs);
        config.setSpecialTarget(0, DataUnit.DataFormat.PARQUET);
        setPartitionMultiplier(3);
        SparkJobResult result = runSparkJob(TruncateLatticeAccount.class, config);
        setPartitionMultiplier(1);

        // Update oldLatticeAccountDU as new base
        oldLatticeAccountDU = result.getTargets().get(0);
        if (result.getTargets().get(1).getCount() > 0) {
            changeLists.add(result.getTargets().get(1));
        }
    }

    private void verticalFetch(List<String> attrs2Add, List<String> attrs2Update) {
        log.info("EnrichLatticeAccount, fetch vertically and merge into LatticeAccount");

        // Merge added & updated attributes and generate ColumnSelection for fetch
        Set<String> changedCols = new LinkedHashSet<>();
        changedCols.addAll(attrs2Add);
        changedCols.addAll(attrs2Update);
        List<Column> colsToFetch = changedCols.stream().map(Column::new).collect(Collectors.toList());
        ColumnSelection cs = new ColumnSelection();
        cs.setColumns(colsToFetch);

        // Get vertical changed rows(all rows - changed rows) from LatticeAccount
        HdfsDataUnit verticalChanged = getVerticalChangedRows();
        HdfsDataUnit fetched = fetch(verticalChanged, cs);
        createFetchChangeList(fetched, ChangeListConstants.VerticalMode);
        // Merge fetched result with existing LatticeAccount Table
        merge(fetched, ChangeListConstants.VerticalMode);
    }

    private void horizontalFetch(HdfsDataUnit changedRows) {
        log.info("EnrichLatticeAccount, fetch horizontally and merge into LatticeAccount");

        // Fetch and select all active attributes
        HdfsDataUnit fetched = fetch(changedRows, allActiveColumns);
        createFetchChangeList(fetched, ChangeListConstants.HorizontalMode);
        // Merge fetched result with existing LatticeAccount Table
        merge(fetched, ChangeListConstants.HorizontalMode);
    }

    private HdfsDataUnit getVerticalChangedRows() {
        log.info("EnrichLatticeAccount, get vertical changed rows from LatticeAccount table");

        FilterByJoinConfig config = new FilterByJoinConfig();
        List<DataUnit> inputs = new LinkedList<>();
        HdfsDataUnit accountTable = accountBatchStore.toHdfsDataUnit("AccountBatchStore");
        // Take current account batch store as base
        inputs.add(accountTable);
        if ((changed != null) && (changed.getCount() > 0)) {
            inputs.add(changed);
        }
        config.setInput(inputs);
        config.setKey(InterfaceName.AccountId.name());
        config.setJoinType("left_anti");
        config.setSelectColumns(Arrays.asList(InterfaceName.AccountId.name(), InterfaceName.LatticeAccountId.name()));
        // Set output format as avro as the match step afterwards only take avro input
        config.setSpecialTarget(0, DataUnit.DataFormat.AVRO);
        setPartitionMultiplier(2);
        SparkJobResult result = runSparkJob(FilterByJoinJob.class, config);
        setPartitionMultiplier(1);
        return result.getTargets().get(0);
    }

    private HdfsDataUnit fetch(HdfsDataUnit inputData, ColumnSelection cs) {
        log.info("EnrichLatticeAccount, fetch & match step");

        String avroDir = inputData.getPath();
        MatchInput matchInput = constructMatchInput(avroDir, cs);
        MatchCommand command = bulkMatchService.match(matchInput, null);
        log.info("Bulk match finished: {}", JsonUtils.serialize(command));

        return bulkMatchService.getResultDataUnit(command, "MatchResult");
    }

    private MatchInput constructMatchInput(String avroDir, ColumnSelection cs) {
        MatchInput matchInput = new MatchInput();
        matchInput.setTenant(new Tenant(customerSpace.getTenantId()));
        matchInput.setOperationalMode(OperationalMode.LDC_MATCH);

        AvroInputBuffer inputBuffer = new AvroInputBuffer();
        inputBuffer.setAvroDir(avroDir);
        matchInput.setInputBuffer(inputBuffer);

        Map<MatchKey, List<String>> keyMap = new HashMap<>();
        keyMap.put(MatchKey.LatticeAccountID, Collections.singletonList(InterfaceName.LatticeAccountId.name()));
        matchInput.setKeyMap(keyMap);
        matchInput.setSkipKeyResolution(true);

        matchInput.setCustomSelection(cs);

        matchInput.setFetchOnly(true);
        matchInput.setDataCloudOnly(true);
        return matchInput;
    }

    private void merge(HdfsDataUnit inputData, String creationMode) {
        log.info("EnrichLatticeAccount, merge fetch result into current LatticeAccount");
        HdfsDataUnit output;
        SparkJobResult result;
        if (inputData == null) { // no need to really merge
            output = (HdfsDataUnit) oldLatticeAccountDU;
        } else {
            MergeLatticeAccountConfig jobConfig = new MergeLatticeAccountConfig();
            switch (creationMode) {
            case ChangeListConstants.HorizontalMode:
            case ChangeListConstants.VerticalMode:
                jobConfig.setMergeMode(creationMode);
                break;
            default:
                log.warn("Unsupported creationMode!");
            }
            jobConfig.setInput(Arrays.asList( //
                    oldLatticeAccountDU, // old table
                    inputData // new table
            ));
            jobConfig.setSpecialTarget(0, DataUnit.DataFormat.PARQUET);
            setPartitionMultiplier(3);
            result = runSparkJob(MergeLatticeAccount.class, jobConfig);
            setPartitionMultiplier(1);
            output = result.getTargets().get(0);
        }

        // Update oldLatticeAccountDU as new base
        oldLatticeAccountDU = output;
    }

    private void mergeChangeList() {
        log.info("EnrichLatticeAccount, merge all generated changelists during updateLatticeAccount");

        if (rebuildDownstream) {
            log.info("Decide to rebuild downstream, no need to build change list");
            latticeAccountChangeListTable = null;
        } else {
            HdfsDataUnit output;
            if (changeLists.size() > 1) {
                MergeImportsConfig config = new MergeImportsConfig();
                config.setInput(changeLists);
                // Since it's for appending, no dedup is needed
                config.setDedupSrc(false);
                config.setJoinKey(null);
                config.setAddTimestamps(false);

                SparkJobResult result = runSparkJob(MergeImportsJob.class, config);
                output = result.getTargets().get(0);
            } else {
                output = (HdfsDataUnit) changeLists.get(0);
            }

            // Save change list table
            String tableName = NamingUtils.timestamp("LatticeAccountChangeList");
            latticeAccountChangeListTable = toTable(tableName, output);
        }

    }

    /**
     * comparing the fetch result with the old table to generate changelist
     */
    private void createFetchChangeList(HdfsDataUnit fetchResult, String creationMode) {
        log.info("EnrichLatticeAccount, create changelist using the fetch results");
        if (rebuildDownstream) {
            log.info("Decide to rebuild downstream, no need to build change list");
        } else {
            List<DataUnit> inputs = new LinkedList<>();
            inputs.add(fetchResult); // toTable: new table
            inputs.add(oldLatticeAccountDU); // fromTable: original table
            ChangeListConfig config = new ChangeListConfig();
            config.setInput(inputs);
            config.setCreationMode(creationMode);
            config.setJoinKey(joinKey);
            config.setExclusionColumns(Arrays.asList( //
                    InterfaceName.CDLCreatedTime.name(), //
                    InterfaceName.CDLUpdatedTime.name(), //
                    joinKey, //
                    InterfaceName.LatticeAccountId.name() //
            ));
            try {
                setPartitionMultiplier(4);
                SparkJobResult result = runSparkJob(CreateChangeListJob.class, config);
                setPartitionMultiplier(1);
                changeLists.add(result.getTargets().get(0));
            } catch (RuntimeException e) {
                log.warn("Failed to create change list, set rebuildDownstream to true.");
                rebuildDownstream = true;
            }
        }
    }

    private void saveLatticeAccountChangelist() {
        if (latticeAccountChangeListTable != null) {
            log.info("EnrichLatticeAccount, save lattice account changelist table");
            metadataProxy.createTable(customerSpace.toString(), latticeAccountChangeListTable.getName(),
                    latticeAccountChangeListTable);
            exportToS3AndAddToContext(latticeAccountChangeListTable, LATTICE_ACCOUNT_CHANGELIST_TABLE_NAME);
        }
    }

    private void saveLatticeAccount() {
        if (newLatticeAccountTable != null) { // LatticeAccount table changed or rebuilt
            log.info("EnrichLatticeAccount, save new LatticeAccount table");
            metadataProxy.createTable(customerSpace.toString(), newLatticeAccountTable.getName(),
                    newLatticeAccountTable);
            dataCollectionProxy.upsertTable(customerSpace.toString(), newLatticeAccountTable.getName(), LatticeAccount,
                    inactive);
            exportToS3AndAddToContext(newLatticeAccountTable, LATTICE_ACCOUNT_TABLE_NAME);
        } else { // No new change -> reuse old version LatticeAccount
            log.info("EnrichLatticeAccount, use current LatticeAccount table as the latest one");
            linkInactiveTable(LatticeAccount);
        }
    }

    private void markRebuildFlag() {
        if (rebuildDownstream) {
            // inform downstream jobs to treat LatticeAccount as rebuild
            putObjectInContext(REBUILD_LATTICE_ACCOUNT, true);
        }
    }

    private List<String> findAttrs2Remove() {
        Set<String> attrs2Remove = new LinkedHashSet<>();
        if (oldLatticeAccountTable != null) {
            attrs2Remove.addAll(Arrays.asList(oldLatticeAccountTable.getAttributeNames()));
            attrs2Remove.removeAll(fetchAttrs);
            // do not remove these attrs
            attrs2Remove.remove(InterfaceName.AccountId.name());
            attrs2Remove.remove(InterfaceName.LatticeAccountId.name());
            attrs2Remove.remove(InterfaceName.CDLCreatedTime.name());
            attrs2Remove.remove(InterfaceName.CDLUpdatedTime.name());
            log.info("Going to remove {} attributes from LatticeAccount: {}", attrs2Remove.size(), attrs2Remove);
        }
        return new LinkedList<>(attrs2Remove);
    }

    private List<String> findAttrs2Add() {
        List<String> attrs2Add = new ArrayList<>(fetchAttrs);
        // do not re-add these attrs
        attrs2Add.remove(InterfaceName.AccountId.name());
        attrs2Add.remove(InterfaceName.LatticeAccountId.name());
        attrs2Add.remove(InterfaceName.CDLCreatedTime.name());
        attrs2Add.remove(InterfaceName.CDLUpdatedTime.name());
        if (oldLatticeAccountTable != null) {
            attrs2Add.removeAll(Arrays.asList(oldLatticeAccountTable.getAttributeNames()));
        }
        log.info("Going to add {} new attributes to LatticeAccount: {}", attrs2Add.size(), attrs2Add);
        return attrs2Add;
    }

    private List<String> findAttrs2Update() {
        Set<String> attrs2Update = new LinkedHashSet<>();
        boolean hasDataCloudMinorChange = //
                Boolean.TRUE.equals(getObjectFromContext(HAS_DATA_CLOUD_MINOR_CHANGE, Boolean.class));
        if (hasDataCloudMinorChange) {
            Set<String> refreshedLDCAttrs = columnMetadataProxy.getAllColumns(getConfiguration().getDataCloudVersion())
                    .stream().filter(column -> RefreshFrequency.WEEK.equals(column.getRefreshFrequency())) //
                    .map(ColumnMetadata::getAttrName).collect(Collectors.toSet());
            attrs2Update.addAll(refreshedLDCAttrs);
            attrs2Update.retainAll(fetchAttrs);
            log.info("Going to update {} LDC attributes due to data cloud minor release.", attrs2Update.size());
        }
        return new LinkedList<>(attrs2Update);
    }

    private ColumnSelection selectActiveDataCloudAttrs() {
        List<Column> colsToFetch = fetchAttrs.stream().map(Column::new).collect(Collectors.toList());
        ColumnSelection cs = new ColumnSelection();
        cs.setColumns(colsToFetch);
        log.info("Added {} attributes to ColumnSelection", colsToFetch.size());
        return cs;
    }

    private List<String> getFetchAttrs() {
        // FIXME: (M38) using serving stores in PA is bad practice.
        // FIXME: need to find a workaround
        List<String> modelAttrs = servingStoreProxy //
                .getAllowedModelingAttrs(customerSpace.toString(), false, inactive) //
                .map(ColumnMetadata::getAttrName) //
                .collectList().block();
        List<String> activatedAttrs = servingStoreProxy
                .getDecoratedMetadata(customerSpace.toString(), BusinessEntity.Account, null, inactive) //
                .filter(cm -> !AttrState.Inactive.equals(cm.getAttrState())) //
                .map(ColumnMetadata::getAttrName) //
                .collectList().block();
        Set<String> validAttrs = new HashSet<>();
        if (CollectionUtils.isNotEmpty(modelAttrs)) {
            validAttrs.addAll(modelAttrs);
        }
        if (CollectionUtils.isNotEmpty(activatedAttrs)) {
            validAttrs.addAll(activatedAttrs);
        }
        // LatticeAccountId is in input, so should exclude from output
        validAttrs.remove(InterfaceName.LatticeAccountId.name());
        log.info("Found {} valid attributes from serving store", validAttrs.size());
        String dataCloudVersion = getConfiguration().getDataCloudVersion();
        List<ColumnMetadata> dcCols = columnMetadataProxy.getAllColumns(dataCloudVersion);
        log.info("Column size from LDC is {}, dataCloudVersion {} ", dcCols.size(), dataCloudVersion);
        boolean isIntentAlertEnabled = isIntentAlertEnabled();
        Set<String> fetchAttrs = new HashSet<>();
        for (ColumnMetadata cm : dcCols) {
            String attr = cm.getAttrName();
            if (validAttrs.contains(attr)) {
                fetchAttrs.add(attr);
            } else if (isIntentAlertEnabled && DEFAULT_FIRMOGRAPHIC_ATTRIBUTES.contains(attr)) {
                fetchAttrs.add(attr);
            }
        }
        // If generate Intent email alert is enabled for this tenant,
        // make sure all those default firmographic attributes are included
        if (isIntentAlertEnabled) {
            for (String attr : DEFAULT_FIRMOGRAPHIC_ATTRIBUTES) {
                if (!fetchAttrs.contains(attr)) {
                    log.error("Attribute {} is missing, which is required for Intent email alert", attr);
                    throw new RuntimeException(
                            String.format("Failed attribute check for Intent email alert, missing %s", attr));
                }
            }
        }
        log.info("Found {} LDC attributes to fetch", fetchAttrs.size());
        return new LinkedList<>(fetchAttrs);
    }

    private boolean isIntentAlertEnabled() {
        return !timeLineProxy.findAll(customerSpace.toString()).isEmpty()
                || !subscriptionProxy.getEmailsByTenantId(customerSpace.getTenantId()).isEmpty();
    }
}
