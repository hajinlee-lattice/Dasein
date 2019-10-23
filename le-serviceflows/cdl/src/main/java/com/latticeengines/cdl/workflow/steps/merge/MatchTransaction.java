package com.latticeengines.cdl.workflow.steps.merge;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MATCH;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.ConsolidateDataTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.ConsolidateDataTransformerConfig.ConsolidateDataTxmfrConfigBuilder;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
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
        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName("MatchTransaction");
        matchTargetTablePrefix = entity.name() + "_Matched";

        List<TransformationStepConfig> steps = new ArrayList<>();
        int mergeStep = 0;
        TransformationStepConfig merge = mergeInputs(getConsolidateDataTxmfrConfig(), matchTargetTablePrefix,
                ETLEngineLoad.LIGHT, null, -1);
        steps.add(merge);
        if (configuration.isEntityMatchEnabled()) {
            bumpEntityMatchStagingVersion();
            String convertBatchStoreTableName = getConvertBatchStoreTableName();
            // if we have convertBatchStore, we should merge and match new import first
            // then merge and match the Table which combined convertBatchStoreTable with the match result of new import
            if (StringUtils.isNotEmpty(convertBatchStoreTableName)) {
                TransformationStepConfig match = match(mergeStep++, null, null);
                merge = mergeInputs(getConsolidateDataTxmfrConfig(), matchTargetTablePrefix,
                        ETLEngineLoad.LIGHT, convertBatchStoreTableName, mergeStep++);
                steps.add(match);
                steps.add(merge);
            }
            TransformationStepConfig match = match(mergeStep, matchTargetTablePrefix, convertBatchStoreTableName);
            steps.add(match);
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

    private TransformationStepConfig match(int inputStep, String matchTargetTable, String convertBatchStoreTableName) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(inputStep));
        if (matchTargetTable != null) {
            setTargetTable(step, matchTargetTable);
        }
        step.setTransformer(TRANSFORMER_MATCH);
        step.setConfiguration(getMatchConfig(convertBatchStoreTableName));
        return step;
    }

    private String getMatchConfig(String convertBatchStoreTableName) {
        // NOTE get all imports just to be safe, currently txn should only have one
        // template
        Set<String> columnNames = getInputTableColumnNames();
        MatchInput matchInput = getBaseMatchInput();
        boolean hasConvertBatchStoreTableName = StringUtils.isNotEmpty(convertBatchStoreTableName);
        if (hasConvertBatchStoreTableName) {
            columnNames.addAll(getTableColumnNames(convertBatchStoreTableName));
            setServingVersionForEntityMatchTenant(matchInput);
        }
        log.info("matchInput is {}.", matchInput);
        if (configuration.isEntityMatchGAOnly()) {
            return MatchUtils.getAllocateIdMatchConfigForAccount(customerSpace.toString(), matchInput, columnNames,
                    Collections.singletonList(InterfaceName.CustomerAccountId.name()), null, hasConvertBatchStoreTableName);
        } else {
            return MatchUtils.getAllocateIdMatchConfigForAccount(customerSpace.toString(), matchInput, columnNames,
                    Collections.singletonList(InterfaceName.CustomerAccountId.name()), newAccountTableName, hasConvertBatchStoreTableName);
        }
    }

    private ConsolidateDataTransformerConfig getConsolidateDataTxmfrConfig() {
        ConsolidateDataTransformerConfig config = getConsolidateDataTxmfrConfig(false, true, true);
        ConsolidateDataTxmfrConfigBuilder builder = new ConsolidateDataTxmfrConfigBuilder(config);
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
