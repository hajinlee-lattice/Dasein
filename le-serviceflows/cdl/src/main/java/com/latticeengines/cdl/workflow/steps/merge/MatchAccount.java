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

import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;

@Component(MatchAccount.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MatchAccount extends BaseSingleEntityMergeImports<ProcessAccountStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(MatchAccount.class);

    static final String BEAN_NAME = "matchAccount";

    private String matchTargetTablePrefix = null;

    private int mergeStep;
    private int matchStep;
    private int fetchOnlyMatchStep;

    @Override
    public PipelineTransformationRequest getConsolidateRequest() {
        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName("MatchAccount");
        matchTargetTablePrefix = entity.name() + "_Matched";

        if (isShortCutMode()) {
            log.info("Found diff table and batch store in context, using short-cut pipeline");
            return null;
        }

        if (configuration.isEntityMatchEnabled()) {
            request.setSteps(entityMatchSteps());
        } else {
            request.setSteps(legacyMatchSteps());
        }
        return request;
    }

    private boolean isShortCutMode() {
        String diffTableNameInContext = getStringValueFromContext(ACCOUNT_DIFF_TABLE_NAME);
        String batchStoreNameInContext = getStringValueFromContext(ACCOUNT_MASTER_TABLE_NAME);
        Table diffTableInContext = StringUtils.isNotBlank(diffTableNameInContext) ? //
                metadataProxy.getTable(customerSpace.toString(), diffTableNameInContext) : null;
        Table batchStoreInContext = StringUtils.isNotBlank(batchStoreNameInContext) ? //
                metadataProxy.getTable(customerSpace.toString(), batchStoreNameInContext) : null;
        return diffTableInContext != null && batchStoreInContext != null;
    }

    @Override
    protected void onPostTransformationCompleted() {
        String targetTableName = getEntityMatchTargetTableName();
        putStringValueInContext(ENTITY_MATCH_ACCOUNT_TARGETTABLE, targetTableName);
        addToListInContext(TEMPORARY_CDL_TABLES, targetTableName, String.class);
    }

    private List<TransformationStepConfig> entityMatchSteps() {
        mergeStep = 0;
        matchStep = 1;
        fetchOnlyMatchStep = 2;

        List<TransformationStepConfig> steps = new ArrayList<>();
        TransformationStepConfig merge = mergeInputs(false, false, true);
        TransformationStepConfig match = match(mergeStep, null);
        TransformationStepConfig fetchOnly = fetchOnlyMatch(matchStep);
        steps.add(merge);
        steps.add(match);
        steps.add(fetchOnly);
        return steps;

    }

    private List<TransformationStepConfig> legacyMatchSteps() {
        List<TransformationStepConfig> steps = new ArrayList<>();
        mergeStep = 0;
        matchStep = 1;
        TransformationStepConfig merge = mergeInputs(false, true, false);
        TransformationStepConfig match = match(mergeStep, matchTargetTablePrefix);
        steps.add(merge);
        steps.add(match);
        return steps;
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
        if (configuration.isEntityMatchEnabled()) {
            return MatchUtils.getAllocateIdMatchConfigForAccount(matchInput, columnNames);
        } else {
            return MatchUtils.getLegacyMatchConfigForAccount(matchInput, columnNames);
        }
    }

    private TransformationStepConfig fetchOnlyMatch(int inputStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(inputStep));
        step.setTransformer(TRANSFORMER_MATCH);
        setTargetTable(step, matchTargetTablePrefix);
        step.setConfiguration(getFetchOnlyMatchConfig());
        return step;
    }

    private String getFetchOnlyMatchConfig() {
        return MatchUtils.getFetchOnlyMatchConfigForAccount(getBaseMatchInput());
    }

    private String getEntityMatchTargetTableName() {
        return TableUtils.getFullTableName(matchTargetTablePrefix, pipelineVersion);
    }
}
