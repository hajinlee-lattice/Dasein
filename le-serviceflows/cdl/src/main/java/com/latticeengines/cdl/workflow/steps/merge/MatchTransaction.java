package com.latticeengines.cdl.workflow.steps.merge;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MATCH;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.config.atlas.ConsolidateDataTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.atlas.ConsolidateDataTransformerConfig.ConsolidateDataTxfmrConfigBuilder;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessTransactionStepConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.serviceflows.workflow.util.ETLEngineLoad;

@Component(MatchTransaction.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MatchTransaction extends BaseSingleEntityMergeImports<ProcessTransactionStepConfiguration> {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(MatchTransaction.class);

    static final String BEAN_NAME = "matchTransaction";

    private String matchTargetTablePrefix = null;
    private String newAccountTableName = NamingUtils.timestamp("NewAccountsFromTxn");

    @Override
    public PipelineTransformationRequest getConsolidateRequest() {
        if (isShortCutMode()) {
            log.info("Found entity match checkpoint in the context, using short-cut pipeline");
            return null;
        } else if (hasNoImportAndNoBatchStore()) {
            log.info("no Import and no batchStore, skip this step.");
            return null;
        }
        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName("MatchTransaction");
        matchTargetTablePrefix = entity.name() + "_Matched";

        List<TransformationStepConfig> steps = new ArrayList<>();

        if (configuration.isEntityMatchEnabled()) {
            bumpEntityMatchStagingVersion();
            List<String> convertedRematchTableNames = getConvertedRematchTableNames();
            if (CollectionUtils.isNotEmpty(inputTableNames)) {
                TransformationStepConfig mergeImport = mergeInputs(getConsolidateDataTxfmrConfig(), null,
                        ETLEngineLoad.LIGHT, null, -1);
                steps.add(mergeImport);
                if (CollectionUtils.isNotEmpty(convertedRematchTableNames)) {
                    // only try to lookup existing account ID for imports, don't care about new
                    // account since later match step will take care of that
                    TransformationStepConfig matchImport = matchTransaction(steps.size() - 1, null, null, false);
                    steps.add(matchImport);
                }
            }
            if (CollectionUtils.isNotEmpty(convertedRematchTableNames)) {
                // when there is no input table, steps.size() - 1 will be -1
                TransformationStepConfig mergeImportAndBatchStore = mergeInputs(
                        getConsolidateDataTxfmrConfig(false, true, true), null, ETLEngineLoad.LIGHT,
                        convertedRematchTableNames, steps.size() - 1);
                steps.add(mergeImportAndBatchStore);
                // If has rematch fake imports, filter out those columns after concat all imports
                TransformationStepConfig filterImports = filterColumnsFromImports(steps.size() - 1);
                steps.add(filterImports);
            }
            TransformationStepConfig matchImportAndBatchStore = matchTransaction(steps.size() - 1,
                    matchTargetTablePrefix, convertedRematchTableNames, true);
            steps.add(matchImportAndBatchStore);
        } else {
            // legacy tenants, just merge imports
            steps.add(mergeInputs(getConsolidateDataTxfmrConfig(), matchTargetTablePrefix, ETLEngineLoad.LIGHT, null,
                    -1));
        }
        log.info("steps are {}.", steps);
        request.setSteps(steps);
        return request;
    }

    @Override
    protected void onPostTransformationCompleted() {
        String targetTableName = TableUtils.getFullTableName(matchTargetTablePrefix, pipelineVersion);
        mergeInputSchema(targetTableName);
        putStringValueInContext(ENTITY_MATCH_TXN_TARGETTABLE, targetTableName);
        addToListInContext(TEMPORARY_CDL_TABLES, targetTableName, String.class);
        Table newAccountTable = metadataProxy.getTableSummary(customerSpace.toString(), newAccountTableName);
        if (newAccountTable != null) {
            putStringValueInContext(ENTITY_MATCH_TXN_ACCOUNT_TARGETTABLE, newAccountTableName);
            addToListInContext(TEMPORARY_CDL_TABLES, newAccountTableName, String.class);
        }
    }

    private boolean isShortCutMode() {
        return Boolean.TRUE.equals(getObjectFromContext(ENTITY_MATCH_COMPLETED, Boolean.class));
    }

    private TransformationStepConfig matchTransaction(int inputStep, String matchTargetTable,
            List<String> convertedRematchTableNames, boolean registerNewAccountTable) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(inputStep));
        if (matchTargetTable != null) {
            setTargetTable(step, matchTargetTable);
        }
        step.setTransformer(TRANSFORMER_MATCH);
        step.setConfiguration(getMatchConfig(convertedRematchTableNames, registerNewAccountTable));
        return step;
    }

    private String getMatchConfig(List<String> convertedRematchTableNames, boolean registerNewAccountTable) {
        // NOTE get all imports just to be safe, currently txn should only have one
        // template
        Set<String> columnNames = getInputTableColumnNames();
        MatchInput matchInput = getBaseMatchInput();
        boolean hasConvertedRematchTables = CollectionUtils.isNotEmpty(convertedRematchTableNames);
        if (hasConvertedRematchTables) {
            convertedRematchTableNames.forEach(tableName -> {
                columnNames.addAll(getTableColumnNames(tableName));
            });
            setRematchVersions(matchInput);
        }
        if (configuration.getEntityMatchConfiguration() != null) {
            log.info("found custom entity match configuration = {}", configuration.getEntityMatchConfiguration());
            matchInput.setEntityMatchConfiguration(configuration.getEntityMatchConfiguration());
        }
        matchInput.setSourceEntity(BusinessEntity.Transaction.name());
        matchInput.setUseTransactAssociate(false);
        log.info("matchInput is {}.", matchInput);
        if (configuration.isEntityMatchGAOnly()) {
            return MatchUtils.getAllocateIdMatchConfigForAccount(customerSpace.toString(), matchInput, columnNames,
                    getSystemIds(BusinessEntity.Account), null, hasConvertedRematchTables, null);
        } else {
            return MatchUtils.getAllocateIdMatchConfigForAccount(customerSpace.toString(), matchInput, columnNames,
                    getSystemIds(BusinessEntity.Account), registerNewAccountTable ? newAccountTableName : null,
                    hasConvertedRematchTables, null);
        }
    }

    private ConsolidateDataTransformerConfig getConsolidateDataTxfmrConfig() {
        ConsolidateDataTransformerConfig config = getConsolidateDataTxfmrConfig(false, true, true);
        ConsolidateDataTxfmrConfigBuilder builder = new ConsolidateDataTxfmrConfigBuilder(config);
        // For PA during entity match migration period: some files are imported
        // with legacy template (having AccountId) while some files are imported
        // after template is upgraded (having CustomerAccountId)
        // The merge job has check whether specified original column (AccountId)
        // exists or not
        // TODO: After all the tenants finish entity match migration, we could
        // get rid of this field rename logic
        if (configuration.isEntityMatchEnabled()) {
            builder.renameSrcFields(
                    new String[][] { { InterfaceName.AccountId.name(), InterfaceName.CustomerAccountId.name() } });
        }
        return builder.build();
    }
}
