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
import com.latticeengines.domain.exposed.spark.cdl.JoinChangeListAccountBatchConfig;
import com.latticeengines.domain.exposed.spark.cdl.MergeImportsConfig;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.serviceflows.workflow.match.BulkMatchService;
import com.latticeengines.spark.exposed.job.cdl.CreateChangeListJob;
import com.latticeengines.spark.exposed.job.cdl.JoinChangeListAccountBatchJob;
import com.latticeengines.spark.exposed.job.cdl.MergeImportsJob;

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
    private Table accountBatchStore;
    private Table accountChangeList;
    private Table oldLatticeAccountTable;
    private Table newLatticeAccountTable;
    private Table fullChangeListTable;
    private String joinKey;
    private String ldcTablePrefix;
    private String fullChangeListPrefix = "FullChangeList";
    private DataUnit accountChangeListDU;
    private DataUnit latticeAccountDU;

    @Override
    public void execute() {
        bootstrap();
        List<Table> tablesInCtx = getTableSummariesFromCtxKeys(customerSpace.toString(),
                Arrays.asList(LATTICE_ACCOUNT_TABLE_NAME, FULL_CHANGELIST_TABLE_NAME));
        shortCutMode = tablesInCtx.stream().noneMatch(Objects::isNull);

        if (shortCutMode) {
            log.info("Found Lattice account and full changelist table in context, go through short-cut mode.");
            String latticeAccountTableName = tablesInCtx.get(0).getName();
            String fullChangelistTableName = tablesInCtx.get(1).getName();
            dataCollectionProxy.upsertTable(customerSpace.toString(), latticeAccountTableName,
                    TableRoleInCollection.LatticeAccount, inactive);
            addToListInContext(TEMPORARY_CDL_TABLES, fullChangelistTableName, String.class);
        } else {
            preExecution();
            // First check if the changelist from MergeAccount exists
            if (accountChangeList != null) {
                HdfsDataUnit initialData = null;
                if (oldLatticeAccountTable != null) { // Old LatticeAccount table exists
                    // Join changelist from MergeAccount with account batch store
                    initialData = join();
                } else { // LatticeAccount table doesn't exist, initial run
                    // Enrich all accounts
                    log.info("account batch store name is {}, path is {} ", accountBatchStore.getName(),
                            accountBatchStore.getExtracts().get(0).getPath());
                    initialData = accountBatchStore.toHdfsDataUnit("AccountBatchStore");
                }

                // Run fetch only bulk match
                HdfsDataUnit enriched = fetch(initialData);

                // Merge with LatticeAccount table, if exists
                HdfsDataUnit merged = merge(enriched);

                // Use old and new LatticeAccount table to generate changelist
                HdfsDataUnit changelist = generateChangeList(merged);

                // Append changelist from previous step to changelist from MergeAccount
                HdfsDataUnit append = append(changelist);

                postExecution();
            } else {
                throw new RuntimeException("Changelist from MergeAccount doesn't exist, can't continue");
            }
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
        }
        if (oldLatticeAccountTable != null) {
            latticeAccountDU = toDataUnit(oldLatticeAccountTable, "OldLatticeAccount");
        }
    }

    protected void postExecution() {
        // Save fullchangelist table
        if (fullChangeListTable != null) {
            log.info("EnrichLatticeAccount, save full changelist table");
            metadataProxy.createTable(customerSpace.toString(), fullChangeListTable.getName(), fullChangeListTable);
            exportToS3AndAddToContext(fullChangeListTable, FULL_CHANGELIST_TABLE_NAME);
            addToListInContext(TEMPORARY_CDL_TABLES, fullChangeListTable.getName(), String.class);
        }

        // Save LatticeAccount table
        if (newLatticeAccountTable != null) {
            log.info("EnrichLatticeAccount, save LatticeAccount table");
            metadataProxy.createTable(customerSpace.toString(), newLatticeAccountTable.getName(),
                    newLatticeAccountTable);
            dataCollectionProxy.upsertTable(customerSpace.toString(), newLatticeAccountTable.getName(),
                    TableRoleInCollection.LatticeAccount, inactive);
            exportToS3AndAddToContext(newLatticeAccountTable, LATTICE_ACCOUNT_TABLE_NAME);
        }

    }

    private HdfsDataUnit join() {
        log.info("EnrichLatticeAccount, join step");
        JoinChangeListAccountBatchConfig config = new JoinChangeListAccountBatchConfig();
        List<DataUnit> inputs = new LinkedList<>();
        inputs.add(accountChangeListDU);
        inputs.add(toDataUnit(accountBatchStore, "AccountBatchStore"));
        config.setInput(inputs);
        config.setJoinKey(joinKey);
        // Get all pair of (AccountId, LatticeAccountId) that need to update LDC
        // attributes
        config.setSelectColumns(Arrays.asList(InterfaceName.AccountId.name(), InterfaceName.LatticeAccountId.name()));

        SparkJobResult result = runSparkJob(JoinChangeListAccountBatchJob.class, config);
        HdfsDataUnit output = result.getTargets().get(0);

        return output;
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

    private HdfsDataUnit merge(HdfsDataUnit inputData) {
        log.info("EnrichLatticeAccount, merge step");
        HdfsDataUnit output = null;
        // Old LatticeAccount table doesn't exist, no need to merge
        if (oldLatticeAccountTable == null) {
            output = inputData;
        } else {
            List<DataUnit> inputs = new LinkedList<>();
            inputs.add(inputData);
            inputs.add(latticeAccountDU);
            MergeImportsConfig config = new MergeImportsConfig();
            config.setInput(inputs);
            config.setDedupSrc(true);
            config.setJoinKey(joinKey);
            config.setAddTimestamps(true);

            SparkJobResult result = runSparkJob(MergeImportsJob.class, config);
            output = result.getTargets().get(0);
        }

        // Upsert LatticeAccount table
        String tableName = NamingUtils.timestamp(ldcTablePrefix);
        newLatticeAccountTable = toTable(tableName, output);

        return output;
    }

    private HdfsDataUnit generateChangeList(HdfsDataUnit inputData) {
        log.info("EnrichLatticeAccount, generate changelist step");
        HdfsDataUnit output = null;
        List<DataUnit> inputs = new LinkedList<>();
        inputs.add(toDataUnit(newLatticeAccountTable, "NewLatticeAccount"));
        if (oldLatticeAccountTable != null) {
            inputs.add(latticeAccountDU);
        }

        ChangeListConfig config = new ChangeListConfig();
        config.setInput(inputs);
        config.setJoinKey(joinKey);
        config.setExclusionColumns(
                Arrays.asList(InterfaceName.CDLCreatedTime.name(), InterfaceName.CDLUpdatedTime.name(), joinKey));
        SparkJobResult result = runSparkJob(CreateChangeListJob.class, config);
        output = result.getTargets().get(0);

        return output;
    }

    private HdfsDataUnit append(HdfsDataUnit inputData) {
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

        return output;
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
