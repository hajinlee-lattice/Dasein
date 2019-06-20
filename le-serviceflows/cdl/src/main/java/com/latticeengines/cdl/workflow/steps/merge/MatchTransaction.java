package com.latticeengines.cdl.workflow.steps.merge;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MATCH;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessTransactionStepConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;

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
        TransformationStepConfig merge = mergeInputs(true, true, false, true, matchTargetTablePrefix,
                InterfaceName.Id.name(), batchStorePrimaryKey, inputTableNames);
        steps.add(merge);
        if (configuration.isEntityMatchEnabled()) {
            bumpEntityMatchStagingVersion();
            merge = mergeInputs(true, false, true);
            TransformationStepConfig match = match(mergeStep, matchTargetTablePrefix);
            steps.add(match);
        }

        request.setSteps(steps);
        return request;
    }

    @Override
    protected void onPostTransformationCompleted() {
        String targetTableName = TableUtils.getFullTableName(matchTargetTablePrefix, pipelineVersion);
        mergeInputSchema(targetTableName);
        putStringValueInContext(ENTITY_MATCH_TXN_TARGETTABLE, targetTableName);
        addToListInContext(TEMPORARY_CDL_TABLES, targetTableName, String.class);
        Table newAccountTable = metadataProxy.getTable(customerSpace.toString(), newAccountTableName);
        if (newAccountTable != null) {
            putStringValueInContext(ENTITY_MATCH_TXN_ACCOUNT_TARGETTABLE, newAccountTableName);
            addToListInContext(TEMPORARY_CDL_TABLES, newAccountTableName, String.class);
        }
    }

    private TransformationStepConfig match(int inputStep, String matchTargetTable) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(inputStep));
        if (matchTargetTable != null) {
            setTargetTable(step, matchTargetTable);
        }
        step.setTransformer(TRANSFORMER_MATCH);
        step.setConfiguration(getMatchConfig());
        return step;
    }

    private String getMatchConfig() {
        MatchInput matchInput = getBaseMatchInput();
        Set<String> columnNames = getInputTableColumnNames(0);
        return MatchUtils.getAllocateIdMatchConfigForAccount(customerSpace.toString(), matchInput, columnNames,
                Collections.singletonList(InterfaceName.CustomerAccountId.name()), newAccountTableName);
    }
}
