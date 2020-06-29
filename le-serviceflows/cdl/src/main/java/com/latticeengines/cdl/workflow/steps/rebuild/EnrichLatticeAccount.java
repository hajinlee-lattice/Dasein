package com.latticeengines.cdl.workflow.steps.rebuild;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

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
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.ChangeListConfig;
import com.latticeengines.domain.exposed.spark.cdl.FilterChangelistConfig;
import com.latticeengines.domain.exposed.spark.cdl.MergeImportsConfig;
import com.latticeengines.domain.exposed.spark.common.CopyConfig;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.serviceflows.workflow.match.BulkMatchService;
import com.latticeengines.spark.exposed.job.cdl.CreateChangeListJob;
import com.latticeengines.spark.exposed.job.cdl.FilterChangelistJob;
import com.latticeengines.spark.exposed.job.cdl.MergeChangeListJob;
import com.latticeengines.spark.exposed.job.cdl.MergeImportsJob;
import com.latticeengines.spark.exposed.job.common.CopyJob;

@Component(EnrichLatticeAccount.BEAN_NAME)
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class EnrichLatticeAccount extends BaseProcessAnalyzeSparkStep<ProcessAccountStepConfiguration> {

    static final String BEAN_NAME = "enrichLatticeAccount";

    private static final Logger log = LoggerFactory.getLogger(EnrichLatticeAccount.class);

    @Inject
    private ColumnMetadataProxy columnMetadataProxy;

    @Inject
    private BulkMatchService bulkMatchService;

    private boolean shortCutMode = false;
    private Table accountChangeList;
    private Table oldLatticeAccountTable = null;
    private Table newLatticeAccountTable = null;
    private Table fullChangeListTable = null;
    private Table fullAccountTable = null;
    private Table accountBatchStore = null;
    private String joinKey;
    private String ldcTablePrefix;
    private String fullAccountTablePrefix = "FullAccount";
    private String fullChangeListPrefix = "FullChangeList";
    private DataUnit accountChangeListDU;
    private DataUnit latticeAccountDU;
    private HdfsDataUnit deleted = null;
    private HdfsDataUnit changed = null;

    @Override
    public void execute() {
        bootstrap();
        List<Table> tablesInCtx = getTableSummariesFromCtxKeys(customerSpace.toString(),
                Arrays.asList(FULL_ACCOUNT_TABLE_NAME, LATTICE_ACCOUNT_TABLE_NAME, FULL_CHANGELIST_TABLE_NAME));
        shortCutMode = tablesInCtx.stream().noneMatch(Objects::isNull);

        if (shortCutMode) {
            log.info("Found Lattice account and full changelist table in context, go through short-cut mode.");
            String fullAccountTableName = tablesInCtx.get(0).getName();
            String latticeAccountTableName = tablesInCtx.get(1).getName();
            String fullChangelistTableName = tablesInCtx.get(2).getName();
            dataCollectionProxy.upsertTable(customerSpace.toString(), latticeAccountTableName,
                    TableRoleInCollection.LatticeAccount, inactive);
            addToListInContext(TEMPORARY_CDL_TABLES, fullChangelistTableName, String.class);
            addToListInContext(TEMPORARY_CDL_TABLES, fullAccountTableName, String.class);
        } else {
            preExecution();

            if (shoudlRebuildLatticeAccount()) {
                log.info("EnrichLatticeAccount, rebuild LatticeAccount and full account table");

                // Enrich with all accounts in account batch
                log.info("account batch store name is {}, path is {} ", accountBatchStore.getName(),
                        accountBatchStore.getExtracts().get(0).getPath());
                HdfsDataUnit accountTable = accountBatchStore.toHdfsDataUnit("AccountBatchStore");
                // Filter account table to get only <AccountId, LatticeAccountId>
                HdfsDataUnit selected = select(accountTable);

                // Build full LatticeAccount table
                HdfsDataUnit fullLatticeAccount = fetch(selected);

                // Build full account table as well since this is full rebuild
                HdfsDataUnit fullAccount = join(accountTable, fullLatticeAccount);

                // Upsert LatticeAccount tables
                String latticeAccountTableName = NamingUtils.timestamp(ldcTablePrefix);
                newLatticeAccountTable = toTable(latticeAccountTableName, fullLatticeAccount);

                // Upsert full account tables
                String fullAccountTableName = NamingUtils.timestamp(fullAccountTablePrefix);
                fullAccountTable = toTable(fullAccountTableName, fullAccount);
            } else {
                log.info("EnrichLatticeAccount, deal with changelist from MergeAccount");

                // Filter changelist from MergeAccount to get
                // 1: <AccountId, LatticeAccountId> pairs with LatticeAccountId added/updated
                // 2: Rows in changelist with LatticeAccountId deleted
                filterChangelist();

                // If no LatticAccountId related change, run postExecution and return
                if ((changed != null && changed.getCount() == 0) && (deleted != null && deleted.getCount() == 0)) {
                    postExecution();
                    return;
                }

                HdfsDataUnit enriched = null;
                // If there are LatticAccountIds changed
                if ((changed != null) && (changed.getCount() > 0)) {
                    // Run fetch only bulk match
                    enriched = fetch(changed);
                }

                // If there are LatticAccountIds deleted
                if ((deleted != null) && (deleted.getCount() > 0)) {
                    // Delete those LatticAccountIds from old LatticeAccount table
                    // by applying the deletion change
                    applyDelete(deleted);
                }

                // Merge with current LatticeAccount table
                merge(enriched);

                // Use old and new LatticeAccount table to generate changelist
                HdfsDataUnit changelist = generateChangeList();

                // Append changelist from previous step to changelist from MergeAccount
                append(changelist);
            }

            postExecution();
        }
    }

    private void preExecution() {
        joinKey = (configuration.isEntityMatchEnabled() && !inMigrationMode()) ? InterfaceName.EntityId.name()
                : InterfaceName.AccountId.name();
        log.info("joinKey {}", joinKey);
        ldcTablePrefix = TableRoleInCollection.LatticeAccount.name();
        accountBatchStore = attemptGetTableRole(BusinessEntity.Account.getBatchStore(), true);
        accountChangeList = getTableSummaryFromKey(customerSpace.toString(), ACCOUNT_CHANGELIST_TABLE_NAME);
        oldLatticeAccountTable = attemptGetTableRole(TableRoleInCollection.LatticeAccount, false);
        if (accountChangeList != null) {
            accountChangeListDU = toDataUnit(accountChangeList, "AccountChangeList");
        } else {
            throw new RuntimeException("Account changelist doesn't exist, can't continue");
        }
        if (oldLatticeAccountTable != null) {
            latticeAccountDU = toDataUnit(oldLatticeAccountTable, "OldLatticeAccount");
        }
    }

    protected void postExecution() {
        // Save fullchangelist table
        if (fullChangeListTable != null) { // There are changes in LatticeAccount table
            log.info("EnrichLatticeAccount, save full changelist table");
            metadataProxy.createTable(customerSpace.toString(), fullChangeListTable.getName(), fullChangeListTable);
            exportToS3AndAddToContext(fullChangeListTable, FULL_CHANGELIST_TABLE_NAME);
            addToListInContext(TEMPORARY_CDL_TABLES, fullChangeListTable.getName(), String.class);
        } else {
            log.info("EnrichLatticeAccount, no new change in LatticeAccount table, use account changelist");
            putStringValueInContext(FULL_CHANGELIST_TABLE_NAME, accountChangeList.getName());
        }

        // Save LatticeAccount table
        if (newLatticeAccountTable != null) { // LatticeAccount table changed or rebuilt
            log.info("EnrichLatticeAccount, save new LatticeAccount table");
            metadataProxy.createTable(customerSpace.toString(), newLatticeAccountTable.getName(),
                    newLatticeAccountTable);
            dataCollectionProxy.upsertTable(customerSpace.toString(), newLatticeAccountTable.getName(),
                    TableRoleInCollection.LatticeAccount, inactive);
            exportToS3AndAddToContext(newLatticeAccountTable, LATTICE_ACCOUNT_TABLE_NAME);
            if (oldLatticeAccountTable != null) {
                // Register old LatticeAccount table for deletion since new table is created
                addToListInContext(TEMPORARY_CDL_TABLES, oldLatticeAccountTable.getName(), String.class);
            } else {
                // Old LatticeAccount table doesn't exist, set rebuild flag for downstream steps
                putObjectInContext(REBUILD_LATTICE_ACCOUNT, true);
            }
        } else { // No new change
            log.info("EnrichLatticeAccount, use current LatticeAccount table as the latest one");
            metadataProxy.createTable(customerSpace.toString(), oldLatticeAccountTable.getName(),
                    oldLatticeAccountTable);
            dataCollectionProxy.upsertTable(customerSpace.toString(), oldLatticeAccountTable.getName(),
                    TableRoleInCollection.LatticeAccount, inactive);
            exportToS3AndAddToContext(oldLatticeAccountTable, LATTICE_ACCOUNT_TABLE_NAME);
        }

        // Save full account table
        if (fullAccountTable != null) {
            exportToS3AndAddToContext(fullAccountTable, FULL_ACCOUNT_TABLE_NAME);
            addToListInContext(TEMPORARY_CDL_TABLES, fullAccountTable.getName(), String.class);
        }
    }

    private Boolean shoudlRebuildLatticeAccount() {
        return (oldLatticeAccountTable == null);
    }

    private HdfsDataUnit select(HdfsDataUnit input) {
        log.info("UpdateAccountExport, select step");

        CopyConfig config = new CopyConfig();
        List<DataUnit> inputs = new LinkedList<>();
        inputs.add(input);
        config.setInput(inputs);
        config.setSelectAttrs(Arrays.asList(InterfaceName.AccountId.name(), InterfaceName.LatticeAccountId.name()));
        SparkJobResult result = runSparkJob(CopyJob.class, config);
        HdfsDataUnit output = result.getTargets().get(0);

        return output;
    }

    private HdfsDataUnit join(HdfsDataUnit latticeAccount, HdfsDataUnit account) {
        log.info("UpdateAccountExport, join step");

        List<DataUnit> joinInputs = new LinkedList<>();
        joinInputs.add(latticeAccount);
        joinInputs.add(account);
        MergeImportsConfig joinConfig = new MergeImportsConfig();
        joinConfig.setInput(joinInputs);
        joinConfig.setDedupSrc(true);
        joinConfig.setJoinKey(joinKey);
        SparkJobResult joinResult = runSparkJob(MergeImportsJob.class, joinConfig);
        HdfsDataUnit joinOutput = joinResult.getTargets().get(0);

        return joinOutput;
    }

    private void filterChangelist() {
        log.info("EnrichLatticeAccount, filter step");

        FilterChangelistConfig config = new FilterChangelistConfig();
        List<DataUnit> inputs = new LinkedList<>();
        inputs.add(accountChangeListDU);
        config.setInput(inputs);
        config.setKey(InterfaceName.AccountId.name());
        config.setColumnId(InterfaceName.LatticeAccountId.name());
        // Get all <AccountId, LatticeAccountId> pair that has
        // LatticeAccountId changed or deleted
        config.setSelectColumns(Arrays.asList(InterfaceName.AccountId.name(), InterfaceName.LatticeAccountId.name()));

        SparkJobResult result = runSparkJob(FilterChangelistJob.class, config);
        changed = result.getTargets().get(0);
        deleted = result.getTargets().get(1);
    }

    private void applyDelete(HdfsDataUnit deletion) {
        // Only apply deletion when current LatticeAccount table exists
        if (latticeAccountDU != null) {
            log.info("EnrichLatticeAccount, apply deletion step");

            ChangeListConfig config = new ChangeListConfig();
            List<DataUnit> inputs = new LinkedList<>();
            inputs.add(deletion); // changelist
            inputs.add(latticeAccountDU); // source table: current LatticeAccount table
            config.setInput(inputs);
            config.setJoinKey(joinKey);
            SparkJobResult result = runSparkJob(MergeChangeListJob.class, config);

            // Update latticeAccountDU which will be used later
            latticeAccountDU = result.getTargets().get(0);
        }
    }

    private HdfsDataUnit fetch(HdfsDataUnit inputData) {
        log.info("EnrichLatticeAccount, fetch & match step");

        String avroDir = inputData.getPath();
        MatchInput matchInput = constructMatchInput(avroDir);
        MatchCommand command = bulkMatchService.match(matchInput, null);
        log.info("Bulk match finished: {}", JsonUtils.serialize(command));
        HdfsDataUnit output = bulkMatchService.getResultDataUnit(command, "MatchResult");

        return output;
    }

    private void merge(HdfsDataUnit inputData) {
        log.info("EnrichLatticeAccount, merge step");

        HdfsDataUnit output = null;
        List<DataUnit> inputs = new LinkedList<>();
        inputs.add(latticeAccountDU); // base
        if (inputData != null) {
            inputs.add(inputData); // new input
        }
        MergeImportsConfig config = new MergeImportsConfig();
        config.setInput(inputs);
        config.setDedupSrc(true);
        config.setJoinKey(joinKey);
        config.setAddTimestamps(true);

        SparkJobResult result = runSparkJob(MergeImportsJob.class, config);
        output = result.getTargets().get(0);

        // Upsert LatticeAccount table
        String tableName = NamingUtils.timestamp(ldcTablePrefix);
        newLatticeAccountTable = toTable(tableName, output);
    }

    private HdfsDataUnit generateChangeList() {
        log.info("EnrichLatticeAccount, generate changelist step");

        HdfsDataUnit output = null;
        List<DataUnit> inputs = new LinkedList<>();
        inputs.add(toDataUnit(newLatticeAccountTable, "NewLatticeAccount")); // toTable: new table
        inputs.add(toDataUnit(oldLatticeAccountTable, "OldLatticeAccount")); // fromTable: original table
        ChangeListConfig config = new ChangeListConfig();
        config.setInput(inputs);
        config.setJoinKey(joinKey);
        config.setExclusionColumns(Arrays.asList(InterfaceName.CDLCreatedTime.name(),
                InterfaceName.CDLUpdatedTime.name(), joinKey, InterfaceName.LatticeAccountId.name()));
        SparkJobResult result = runSparkJob(CreateChangeListJob.class, config);
        output = result.getTargets().get(0);

        return output;
    }

    private void append(HdfsDataUnit inputData) {
        if (inputData != null) {
            log.info("EnrichLatticeAccount, merge changelist step");
            List<DataUnit> inputs = new LinkedList<>();
            inputs.add(inputData);
            inputs.add(accountChangeListDU);
            MergeImportsConfig config = new MergeImportsConfig();
            config.setInput(inputs);
            // Since it's for appending, no dedup is needed
            config.setDedupSrc(false);
            config.setJoinKey(null);
            config.setAddTimestamps(false);

            SparkJobResult result = runSparkJob(MergeImportsJob.class, config);
            HdfsDataUnit output = result.getTargets().get(0);

            // Create full change list
            String tableName = NamingUtils.timestamp(fullChangeListPrefix);
            fullChangeListTable = toTable(tableName, output);
        }
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
        // Get all attributes from LDC
        String dataCloudVersion = getConfiguration().getDataCloudVersion();
        List<ColumnMetadata> dcCols = columnMetadataProxy.getAllColumns(dataCloudVersion);
        log.info("Column size from LDC is {}, dataCloudVersion {} ", dcCols.size(), dataCloudVersion);
        List<Column> colsToFetch = new ArrayList<>();
        for (ColumnMetadata cm : dcCols) {
            // only consider active attributes
            if (useInternalAttrs() || canBeUsedInModelOrSegment(cm) || isNotInternalAttr(cm)) {
                colsToFetch.add(new Column(cm.getAttrName()));
            }
        }
        ColumnSelection cs = new ColumnSelection();
        cs.setColumns(colsToFetch);
        log.info("Added {} attributes to ColumnSelection", colsToFetch.size());
        return cs;
    }

    private boolean useInternalAttrs() {
        return configuration.isAllowInternalEnrichAttrs();
    }

    private boolean isNotInternalAttr(ColumnMetadata columnMetadata) {
        return !Boolean.TRUE.equals(columnMetadata.getCanInternalEnrich());
    }

    private boolean canBeUsedInModelOrSegment(ColumnMetadata columnMetadata) {
        return columnMetadata.isEnabledFor(ColumnSelection.Predefined.Model)
                || columnMetadata.isEnabledFor(ColumnSelection.Predefined.Segment);
    }
}
