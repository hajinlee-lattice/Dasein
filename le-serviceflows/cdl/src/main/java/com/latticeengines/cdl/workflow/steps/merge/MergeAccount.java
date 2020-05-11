package com.latticeengines.cdl.workflow.steps.merge;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.Tag;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.serviceflows.workflow.util.ScalingUtils;

@Component(MergeAccount.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MergeAccount extends BaseSingleEntityMergeImports<ProcessAccountStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(MergeAccount.class);

    static final String BEAN_NAME = "mergeAccount";

    private int upsertStep;
    private int diffStep;
    private int changeListStep;

    private String diffTableNameInContext;
    private String chgListTableNameInContext;
    private String reportChgListTableNameInContext;
    private String batchStoreNameInContext;
    private String systemBatchStoreNameInContext;

    private boolean shortCutMode;

    private String matchedAccountTable;
    // new accounts generated by non-account matches
    private String newAccountTableFromContactMatch;
    private String newAccountTableFromTxnMatch;

    private boolean noImports;

    @Override
    protected void initializeConfiguration() {
        super.initializeConfiguration();
        List<String> accountTables = !hasSystemBatch
                ? Arrays.asList(ACCOUNT_DIFF_TABLE_NAME, ACCOUNT_MASTER_TABLE_NAME, ACCOUNT_CHANGELIST_TABLE_NAME,
                        ACCOUNT_REPORT_CHANGELIST_TABLE_NAME)
                : Arrays.asList(ACCOUNT_DIFF_TABLE_NAME, ACCOUNT_MASTER_TABLE_NAME, SYSTEM_ACCOUNT_MASTER_TABLE_NAME,
                        ACCOUNT_CHANGELIST_TABLE_NAME, ACCOUNT_REPORT_CHANGELIST_TABLE_NAME);
        List<Table> tablesInCtx = getTableSummariesFromCtxKeys(customerSpace.toString(), accountTables);
        shortCutMode = tablesInCtx != null && tablesInCtx.stream().noneMatch(Objects::isNull);
        if (shortCutMode) {
            log.info("Found diff table and batch store in context, using short-cut pipeline");
            shortCutMode = true;
            diffTableNameInContext = tablesInCtx.get(0).getName();
            batchStoreNameInContext = tablesInCtx.get(1).getName();
            systemBatchStoreNameInContext = tablesInCtx.size() > 4 ? tablesInCtx.get(2).getName() : null;
            chgListTableNameInContext = tablesInCtx.size() > 4 ? tablesInCtx.get(3).getName()
                    : tablesInCtx.get(2).getName();
            reportChgListTableNameInContext = tablesInCtx.size() > 4 ? tablesInCtx.get(4).getName()
                    : tablesInCtx.get(3).getName();
            diffTableName = diffTableNameInContext;
            changeListTableName = chgListTableNameInContext;
            reportChangeListTableName = reportChgListTableNameInContext;
        } else {
            matchedAccountTable = getStringValueFromContext(ENTITY_MATCH_ACCOUNT_TARGETTABLE);
            newAccountTableFromContactMatch = getStringValueFromContext(ENTITY_MATCH_CONTACT_ACCOUNT_TARGETTABLE);
            newAccountTableFromTxnMatch = getStringValueFromContext(ENTITY_MATCH_TXN_ACCOUNT_TARGETTABLE);
            double newTableSize = 0.0D;
            for (String key : Arrays.asList( //
                    ENTITY_MATCH_ACCOUNT_TARGETTABLE, //
                    ENTITY_MATCH_CONTACT_ACCOUNT_TARGETTABLE, //
                    ENTITY_MATCH_TXN_ACCOUNT_TARGETTABLE)) {
                newTableSize += getTableSize(key);
            }
            newTableSize += sumTableSizeFromMapCtx(ENTITY_MATCH_STREAM_ACCOUNT_TARGETTABLE);
            double oldTableSize = ScalingUtils.getTableSizeInGb(yarnConfiguration, masterTable);
            scalingMultiplier = ScalingUtils.getMultiplier(oldTableSize + newTableSize);
            log.info(String.format("Adjust scalingMultiplier=%d based on the size of two tables %.2f g.", //
                    scalingMultiplier, oldTableSize + newTableSize));
        }
    }

    @Override
    public PipelineTransformationRequest getConsolidateRequest() {
        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName("MergeAccount");

        if (shortCutMode) {
            request.setSteps(shortCutSteps());
        } else {
            request.setSteps(regularSteps());
        }

        return request;
    }

    private List<TransformationStepConfig> regularSteps() {
        List<TransformationStepConfig> steps;
        if (configuration.isEntityMatchEnabled()) {
            steps = entityMatchSteps();
        } else {
            steps = legacySteps();
        }
        return steps;
    }

    private List<TransformationStepConfig> entityMatchSteps() {
        List<TransformationStepConfig> extracts = new ArrayList<>();
        List<Integer> extractSteps = new ArrayList<>();
        if (StringUtils.isNotBlank(newAccountTableFromContactMatch)) {
            extracts.add(extractNewAccount(newAccountTableFromContactMatch,
                    getStringValueFromContext(ENTITY_MATCH_CONTACT_TARGETTABLE)));
            extractSteps.add(extractSteps.size());
        }
        if (StringUtils.isNotBlank(newAccountTableFromTxnMatch)) {
            extracts.add(extractNewAccount(newAccountTableFromTxnMatch,
                    getStringValueFromContext(ENTITY_MATCH_TXN_TARGETTABLE)));
            extractSteps.add(extractSteps.size());
        }
        addEmbeddedEntitiesFromActivityStream(extracts, extractSteps, BusinessEntity.Account,
                ENTITY_MATCH_STREAM_ACCOUNT_TARGETTABLE, this::extractNewAccount);

        List<TransformationStepConfig> steps = new ArrayList<>(extracts);

        int mergeStep = extractSteps.size();
        noImports = CollectionUtils.isEmpty(extractSteps) && StringUtils.isBlank(matchedAccountTable);
        if (!noImports) {
            TransformationStepConfig merge = dedupAndMerge(InterfaceName.EntityId.name(), //
                    CollectionUtils.isEmpty(extractSteps) ? null : extractSteps, //
                    StringUtils.isBlank(matchedAccountTable) ? null : Collections.singletonList(matchedAccountTable), //
                    Collections.singletonList(InterfaceName.CustomerAccountId.name()));
            steps.add(merge);
        } else {
            if (!hasSystemBatch) {
                throw new IllegalArgumentException("No input to be merged, and no soft delete needed!");
            } else {
                log.warn("There's no import!");
            }
        }

        TransformationStepConfig upsertSystem = null;
        TransformationStepConfig upsert;
        if (hasSystemBatch) {
            if (noImports) {
                upsertSystem = upsertSystemBatch(-1, true);
                upsert = mergeSystemBatch(0, true);
                steps.add(upsertSystem);
                steps.add(upsert);
                return steps;
            }
            upsertSystem = upsertSystemBatch(mergeStep, true);
            upsert = mergeSystemBatch(mergeStep + 1, true);
            upsertStep = mergeStep + 2;
            diffStep = mergeStep + 3;
            changeListStep = mergeStep + 4;
        } else {
            upsert = upsertMaster(true, mergeStep, true);
            upsertStep = mergeStep + 1;
            diffStep = mergeStep + 2;
            changeListStep = mergeStep + 3;
        }

        // in migration mode, need to use AccountId because legacy batch store won't
        // have EntityId column
        String joinKey = inMigrationMode() ? InterfaceName.AccountId.name() : InterfaceName.EntityId.name();
        TransformationStepConfig diff = diff(mergeStep, upsertStep);
        TransformationStepConfig changeList = createChangeList(upsertStep, joinKey);
        TransformationStepConfig reportChangeList = reportChangeList(changeListStep);
        TransformationStepConfig report = reportDiff(diffStep);
        if (upsertSystem != null) {
            steps.add(upsertSystem);
        }
        steps.add(upsert);
        steps.add(diff);
        steps.add(changeList);
        steps.add(reportChangeList);
        steps.add(report);

        return steps;
    }

    private List<TransformationStepConfig> legacySteps() {
        List<TransformationStepConfig> steps = new ArrayList<>();

        upsertStep = 0;
        diffStep = 1;
        changeListStep = 2;
        TransformationStepConfig upsert = upsertMaster(false, matchedAccountTable);
        TransformationStepConfig diff = diff(matchedAccountTable, upsertStep);
        TransformationStepConfig changeList = createChangeList(upsertStep, InterfaceName.AccountId.name());
        TransformationStepConfig reportChangeList = reportChangeList(changeListStep);
        TransformationStepConfig report = reportDiff(diffStep);
        steps.add(upsert);
        steps.add(diff);
        steps.add(changeList);
        steps.add(reportChangeList);
        steps.add(report);

        return steps;
    }

    private List<TransformationStepConfig> shortCutSteps() {
        TransformationStepConfig report = reportDiff(diffTableName);
        List<TransformationStepConfig> steps = new ArrayList<>();
        steps.add(report);
        return steps;
    }

    // for Account batch store
    @Override
    protected void enrichTableSchema(Table table) {
        Map<String, Attribute> attrsToInherit = new HashMap<>();
        addAttrsToMap(attrsToInherit, inputMasterTableName);
        addAttrsToMap(attrsToInherit, matchedAccountTable);
        addAttrsToMap(attrsToInherit, newAccountTableFromContactMatch);
        addAttrsToMap(attrsToInherit, newAccountTableFromTxnMatch);
        updateAttrs(table, attrsToInherit);
        table.getAttributes().forEach(attr -> {
            attr.setTags(Tag.INTERNAL);
            if (configuration.isEntityMatchEnabled() && InterfaceName.AccountId.name().equals(attr.getName())) {
                attr.setDisplayName("Atlas Account ID");
            }
        });
        metadataProxy.updateTable(customerSpace.toString(), table.getName(), table);
    }

    @Override
    protected void onPostTransformationCompleted() {
        super.onPostTransformationCompleted();
        if (hasSystemBatch) {
            String systemBatchStoreTableName = dataCollectionProxy.getTableName(customerSpace.toString(),
                    systemBatchStore, inactive);
            exportToS3AndAddToContext(systemBatchStoreTableName, SYSTEM_ACCOUNT_MASTER_TABLE_NAME);
        }
        String batchStoreTableName = dataCollectionProxy.getTableName(customerSpace.toString(), batchStore, inactive);
        checkAttributeLimit(batchStoreTableName, configuration.isEntityMatchEnabled());
        exportToS3AndAddToContext(batchStoreTableName, ACCOUNT_MASTER_TABLE_NAME);
        if (!noImports) {
            if (StringUtils.isNotBlank(changeListTableName)) {
                exportToS3AndAddToContext(changeListTableName, ACCOUNT_CHANGELIST_TABLE_NAME);
            }
            if (StringUtils.isNotBlank(reportChangeListTableName)) {
                exportToS3AndAddToContext(reportChangeListTableName, ACCOUNT_REPORT_CHANGELIST_TABLE_NAME);
            }
            exportToS3AndAddToContext(diffTableName, ACCOUNT_DIFF_TABLE_NAME);

        }
        TableRoleInCollection role = TableRoleInCollection.ConsolidatedAccount;
        exportToDynamo(batchStoreTableName, role.getPartitionKey(), role.getRangeKey());
    }

    @Override
    protected String getSystemBatchStoreName() {
        return systemBatchStoreNameInContext != null ? systemBatchStoreNameInContext
                : TableUtils.getFullTableName(systemBatchStoreTablePrefix, pipelineVersion);
    }

    @Override
    protected String getBatchStoreName() {
        if (shortCutMode) {
            return batchStoreNameInContext;
        } else {
            return TableUtils.getFullTableName(batchStoreTablePrefix, pipelineVersion);
        }
    }

    @Override
    protected String getDiffTableName() {
        if (shortCutMode) {
            return diffTableNameInContext;
        } else {
            return TableUtils.getFullTableName(diffTablePrefix, pipelineVersion);
        }
    }

    @Override
    protected String getChangeListTableName() {
        if (shortCutMode) {
            return chgListTableNameInContext;
        } else {
            return TableUtils.getFullTableName(changeListTablePrefix, pipelineVersion);
        }
    }

    @Override
    protected String getReportChangeListTableName() {
        if (shortCutMode) {
            return reportChgListTableNameInContext;
        } else {
            return TableUtils.getFullTableName(reportChangeListTablePrefix, pipelineVersion);
        }
    }

    private TransformationStepConfig extractNewAccount(String newAccountTable, String matchTargetTable) {
        List<String> systemIds = new ArrayList<>();
        systemIds.add(InterfaceName.CustomerAccountId.name());
        systemIds.addAll(getSystemIds(BusinessEntity.Account));

        return getExtractNewEntitiesStepFactory(BusinessEntity.Account, InterfaceName.AccountId, systemIds)
                .apply(newAccountTable, matchTargetTable);
    }

}
