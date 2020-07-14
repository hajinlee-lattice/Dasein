package com.latticeengines.cdl.workflow.steps.rebuild;

import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ConsolidatedAccount;
import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.LatticeAccount;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
import com.latticeengines.domain.exposed.spark.common.FilterChangelistConfig;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.serviceflows.workflow.match.BulkMatchService;
import com.latticeengines.spark.exposed.job.cdl.MergeImportsJob;
import com.latticeengines.spark.exposed.job.cdl.MergeLatticeAccount;
import com.latticeengines.spark.exposed.job.cdl.TruncateLatticeAccount;
import com.latticeengines.spark.exposed.job.common.CopyJob;
import com.latticeengines.spark.exposed.job.common.CreateChangeListJob;
import com.latticeengines.spark.exposed.job.common.FilterChangelistJob;

@Lazy
@Component(EnrichLatticeAccount.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
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

    private Table oldLatticeAccountTable = null;
    private Table newLatticeAccountTable = null;
    private Table latticeAccountChangeListTable = null;
    private Table accountBatchStore = null;
    private String joinKey;
    private String ldcTablePrefix;
    private DataUnit accountChangeListDU;
    private DataUnit oldLatticeAccountDU;
    private HdfsDataUnit deleted = null;
    private HdfsDataUnit changed = null;

    private List<String> fetchAttrs;
    private boolean fetchAll;
    private boolean rebuildDownstream;
    private List<DataUnit> changeLists = new ArrayList<>();

    @Override
    public void execute() {
        bootstrap();
        if (shouldDoNothing()) {
            linkInactiveTable(LatticeAccount);
        } else {
            Table tableInCtx = getTableSummaryFromKey(customerSpaceStr.toLowerCase(), LATTICE_ACCOUNT_TABLE_NAME);
            boolean shortCutMode = tableInCtx != null;
            if (shortCutMode) {
                // LATTICE_ACCOUNT_CHANGELIST_TABLE_NAME exists for sure, if applicable
                log.info("Found Lattice account in context, go through short-cut mode.");
                String latticeAccountTableName = tableInCtx.getName();
                dataCollectionProxy.upsertTable(customerSpace.toString(), latticeAccountTableName,
                        LatticeAccount, inactive);
            } else {
                preExecution();
                buildLatticeAccount();
                postExecution();
            }
        }
    }

    private boolean shouldDoNothing() {
        // some special tenants do not want to use LDC attributes
        boolean noLDC = batonService.shouldSkipFuzzyMatchInPA(customerSpace.getTenantId());
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
            boolean doNothing = !(hasAccountChange || fetchAll|| rebuildDownstream);
            log.info("hasAccountChange={}, fetchAll={}, rebuildDownstream={}: doNothing={}",
                    hasAccountChange, fetchAll, rebuildDownstream, doNothing);
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
        boolean hasDataCloudMinorChange = //
                Boolean.TRUE.equals(getObjectFromContext(HAS_DATA_CLOUD_MINOR_CHANGE, Boolean.class));
        boolean hasAttrs2Add = !findAttrs2Add().isEmpty();
        Table accountChangeList = getTableSummaryFromKey(customerSpace.toString(), ACCOUNT_CHANGELIST_TABLE_NAME);
        boolean missAccountChangeList = isChanged(ConsolidatedAccount) && (accountChangeList == null);
        boolean shouldFetchAll = hasDataCloudMajorChange || hasDataCloudMinorChange || hasAttrs2Add || missAccountChangeList;
        log.info("hasDataCloudMajorChange={}, hasDataCloudMinorChange={}, hasAttrs2Add={}, " + //
                        "missAccountChangeList={}: shouldFetchAll={}", //
                hasDataCloudMajorChange, hasDataCloudMinorChange, hasAttrs2Add, missAccountChangeList, shouldFetchAll);
        return shouldFetchAll;
    }

    private void preExecution() {
        joinKey = InterfaceName.AccountId.name();
        log.info("joinKey {}", joinKey);
        ldcTablePrefix = LatticeAccount.name();
        accountBatchStore = attemptGetTableRole(BusinessEntity.Account.getBatchStore(), true);
    }

    private void buildLatticeAccount() {
        if (rebuildDownstream) {
            log.info("EnrichLatticeAccount, rebuild LatticeAccount and full account table");
            rebuildLatticeAccount();
        } else {
            log.info("EnrichLatticeAccount, deal with changelist from MergeAccount");
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
        HdfsDataUnit fetched = fetch(selected);

        // convert to parquet
        CopyConfig copyConfig = new CopyConfig();
        copyConfig.setInput(Collections.singletonList(fetched));
        copyConfig.setSpecialTarget(0, DataUnit.DataFormat.PARQUET);
        SparkJobResult result = runSparkJob(CopyJob.class, copyConfig);
        HdfsDataUnit newLatticeAccount = result.getTargets().get(0);

        // save LatticeAccount table
        String latticeAccountTableName = NamingUtils.timestamp(ldcTablePrefix);
        newLatticeAccountTable = toTable(latticeAccountTableName, newLatticeAccount);
    }

    private void updateLatticeAccount() {
        boolean hasNewFetch;
        boolean hasDelete = false;

        Table accountChangeList = getTableSummaryFromKey(customerSpace.toString(), ACCOUNT_CHANGELIST_TABLE_NAME);
        Preconditions.checkNotNull(accountChangeList, "Must have account change list.");
        accountChangeListDU = toDataUnit(accountChangeList, "AccountChangeList");

        HdfsDataUnit enriched = enrich();
        hasNewFetch = (enriched != null);

        if (!fetchAll) {
            // If there are LatticeAccountIds to be deleted or attributes to be removed
            List<String> attrs2Remove = findAttrs2Remove();
            if (CollectionUtils.isNotEmpty(attrs2Remove) || (deleted != null && deleted.getCount() > 0)) {
                hasDelete = true;
                applyDelete(deleted, attrs2Remove);
            }
        }

        if (hasNewFetch) {
            createFetchChangeList(enriched);
        }

        if (hasNewFetch || hasDelete) {
            // even it was fetch all, these steps are still useful
            // Merge with current LatticeAccount table
            merge(enriched);
            // Use old and new LatticeAccount table to generate changelist
            mergeChangeList();
        } else {
            log.info("There is no change to LatticeAccount");
        }
    }

    /**
     * When returning null, it means no fetch happened
     */
    private HdfsDataUnit enrich() {
        HdfsDataUnit enriched = null;
        if (fetchAll) {
            // even in update mode, still need to fetch for all Accounts
            rebuildLatticeAccount();
            enriched = newLatticeAccountTable.toHdfsDataUnit("Enriched");
        } else {
            // Filter changelist from MergeAccount to get
            // 1: <AccountId, LatticeAccountId> pairs with LatticeAccountId added/updated
            // 2: Rows in changelist with LatticeAccountId deleted
            filterChangelist();
            // If there are LatticeAccountIds changed
            if ((changed != null) && (changed.getCount() > 0)) {
                // Run fetch only bulk match
                enriched = fetch(changed);
            } else {
                log.info("Nothing needs to be fetched from LDC.");
            }
        }
        return enriched;
    }

    protected void postExecution() {
        saveLatticeAccountChangelist();
        saveLatticeAccount();
        markRebuildFlag();
    }

    private void saveLatticeAccountChangelist() {
        if (latticeAccountChangeListTable != null) {
            log.info("EnrichLatticeAccount, save lattice account changelist table");
            metadataProxy.createTable(customerSpace.toString(), latticeAccountChangeListTable.getName(), latticeAccountChangeListTable);
            exportToS3AndAddToContext(latticeAccountChangeListTable, LATTICE_ACCOUNT_CHANGELIST_TABLE_NAME);
            addToListInContext(TEMPORARY_CDL_TABLES, latticeAccountChangeListTable.getName(), String.class);
        }
    }

    private void saveLatticeAccount() {
        if (newLatticeAccountTable != null) { // LatticeAccount table changed or rebuilt
            log.info("EnrichLatticeAccount, save new LatticeAccount table");
            metadataProxy.createTable(customerSpace.toString(), newLatticeAccountTable.getName(),
                    newLatticeAccountTable);
            dataCollectionProxy.upsertTable(customerSpace.toString(), newLatticeAccountTable.getName(),
                    LatticeAccount, inactive);
            exportToS3AndAddToContext(newLatticeAccountTable, LATTICE_ACCOUNT_TABLE_NAME);
        } else { // No new change -> reuse old version LatticeAccount
            log.info("EnrichLatticeAccount, use current LatticeAccount table as the latest one");
            linkInactiveTable(LatticeAccount);
            putStringValueInContext(LATTICE_ACCOUNT_TABLE_NAME, oldLatticeAccountTable.getName());
        }
    }

    private void markRebuildFlag() {
        if (rebuildDownstream) {
            // inform downstream jobs to treat LatticeAccount as rebuild
            putObjectInContext(REBUILD_LATTICE_ACCOUNT, true);
        }
    }

    private HdfsDataUnit select(HdfsDataUnit input) {
        log.info("UpdateAccountExport, select step");

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

    // find <AccountId, LatticeAccountId> that has changed or deleted
    private void filterChangelist() {
        log.info("EnrichLatticeAccount, filter step");

        FilterChangelistConfig config = new FilterChangelistConfig();
        List<DataUnit> inputs = new LinkedList<>();
        inputs.add(accountChangeListDU);
        config.setInput(inputs);
        config.setKey(InterfaceName.AccountId.name());
        config.setColumnId(InterfaceName.LatticeAccountId.name());
        config.setSelectColumns(Arrays.asList(InterfaceName.AccountId.name(), InterfaceName.LatticeAccountId.name()));

        SparkJobResult result = runSparkJob(FilterChangelistJob.class, config);
        changed = result.getTargets().get(0);
        deleted = result.getTargets().get(1);
        log.info("There are {} accounts have changed LatticeAccountId", changed.getCount());
        log.info("There are {} accounts deleted", deleted.getCount());
    }

    private void applyDelete(HdfsDataUnit deletion, List<String> attrs2Remove) {
        // Only apply deletion when current LatticeAccount table exists
        if (oldLatticeAccountDU != null) {
            log.info("EnrichLatticeAccount, apply deletion step");

            TruncateLatticeAccountConfig config = new TruncateLatticeAccountConfig();
            config.setRemoveAttrs(attrs2Remove);
            config.setIgnoreAttrs(Arrays.asList( //
                    InterfaceName.CDLCreatedTime.name(), //
                    InterfaceName.CDLUpdatedTime.name(), //
                    joinKey, //
                    InterfaceName.LatticeAccountId.name() //
            ));
            if ((changed != null) && (changed.getCount() > 0)) {
                // enrich step will handle remove attrs change list
                config.setIgnoreRemoveAttrsChangeList(true);
            }
            List<DataUnit> inputs = new LinkedList<>();
            inputs.add(oldLatticeAccountDU); // source table: current LatticeAccount table
            if (deletion != null && deletion.getCount() > 0) {
                inputs.add(deletion); // delete changelist
            }
            config.setInput(inputs);
            config.setSpecialTarget(0, DataUnit.DataFormat.PARQUET);
            SparkJobResult result = runSparkJob(TruncateLatticeAccount.class, config);

            // Update latticeAccountDU as new merge base
            oldLatticeAccountDU = result.getTargets().get(0);
            if (result.getTargets().get(1).getCount() > 0) {
                changeLists.add(result.getTargets().get(1));
            }
        }
    }

    private HdfsDataUnit fetch(HdfsDataUnit inputData) {
        log.info("EnrichLatticeAccount, fetch & match step");

        String avroDir = inputData.getPath();
        MatchInput matchInput = constructMatchInput(avroDir);
        MatchCommand command = bulkMatchService.match(matchInput, null);
        log.info("Bulk match finished: {}", JsonUtils.serialize(command));

        return bulkMatchService.getResultDataUnit(command, "MatchResult");
    }

    /**
     * comparing the fetch result with the old table
     */
    private void createFetchChangeList(HdfsDataUnit fetchResult) {
        log.info("EnrichLatticeAccount, generate createFetchChangeList step");
        List<DataUnit> inputs = new LinkedList<>();
        inputs.add(fetchResult); // toTable: new table
        inputs.add(oldLatticeAccountDU); // fromTable: original table
        ChangeListConfig config = new ChangeListConfig();
        config.setInput(inputs);
        config.setCreationMode(ChangeListConstants.VerticalMode);
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
        changeLists.add(result.getTargets().get(0));
    }

    private void merge(HdfsDataUnit inputData) {
        log.info("EnrichLatticeAccount, merge step");
        HdfsDataUnit output;
        if (inputData == null) { // no need to really merge
            output = (HdfsDataUnit) oldLatticeAccountDU;
        } else {
            MergeLatticeAccountConfig jobConfig = new MergeLatticeAccountConfig();
            jobConfig.setInput(Arrays.asList(
                    oldLatticeAccountDU, // old table
                    inputData // new table
            ));
            jobConfig.setSpecialTarget(0, DataUnit.DataFormat.PARQUET);
            setPartitionMultiplier(4);
            SparkJobResult result = runSparkJob(MergeLatticeAccount.class, jobConfig);
            setPartitionMultiplier(1);
            output = result.getTargets().get(0);
        }
        String tableName = NamingUtils.timestamp(ldcTablePrefix);
        newLatticeAccountTable = toTable(tableName, output);
    }

    private List<String> findAttrs2Remove() {
        List<String> attrs2Remove = new ArrayList<>();
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
        return attrs2Remove;
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

    private void mergeChangeList() {
        log.info("EnrichLatticeAccount, merge LatticeAccount changelist step");

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

    private MatchInput constructMatchInput(String avroDir) {
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

        matchInput.setCustomSelection(selectActiveDataCloudAttrs());

        matchInput.setFetchOnly(true);
        matchInput.setDataCloudOnly(true);
        return matchInput;
    }

    private ColumnSelection selectActiveDataCloudAttrs() {
        List<Column> colsToFetch = fetchAttrs.stream().map(Column::new).collect(Collectors.toList());
        ColumnSelection cs = new ColumnSelection();
        cs.setColumns(colsToFetch);
        log.info("Added {} attributes to ColumnSelection", colsToFetch.size());
        return cs;
    }

    private List<String> getFetchAttrs() {
        //FIXME: (M38) using serving stores in PA is bad practice. need to find a workaround
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
        List<String> fetchAttrs = new ArrayList<>();
        for (ColumnMetadata cm : dcCols) {
            if (validAttrs.contains(cm.getAttrName())) {
                fetchAttrs.add(cm.getAttrName());
            }
        }
        log.info("Found {} LDC attributes to fetch", fetchAttrs.size());
        return fetchAttrs;
    }

}
