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

import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;

@Component(MatchAccount.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MatchAccount extends BaseSingleEntityMergeImports<ProcessAccountStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(MatchAccount.class);

    static final String BEAN_NAME = "matchAccount";

    private String matchTargetTablePrefix = null;

    @Override
    public PipelineTransformationRequest getConsolidateRequest() {
        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName("MatchAccount");
        matchTargetTablePrefix = entity.name() + "_Matched";

        if (isShortCutMode()) {
            log.info("Found diff table and batch store in context, using short-cut pipeline");
            return null;
        }

        List<TransformationStepConfig> steps = new ArrayList<>();
        int mergeStep = 0;
        TransformationStepConfig merge;
        if (configuration.isEntityMatchEnabled()) {
            bumpEntityMatchStagingVersion();
            merge = concatImports(null);
        } else {
            merge = dedupAndConcatImports(InterfaceName.AccountId.name());
        }
        TransformationStepConfig match = match(mergeStep, matchTargetTablePrefix);
        steps.add(merge);
        steps.add(match);

        request.setSteps(steps);
        return request;
    }

    private boolean isShortCutMode() {
        return Boolean.TRUE.equals(getObjectFromContext(ENTITY_MATCH_COMPLETED, Boolean.class));
    }

    @Override
    protected void onPostTransformationCompleted() {
        String targetTableName = TableUtils.getFullTableName(matchTargetTablePrefix, pipelineVersion);
        mergeInputSchema(targetTableName);
        putStringValueInContext(ENTITY_MATCH_ACCOUNT_TARGETTABLE, targetTableName);
        addToListInContext(TEMPORARY_CDL_TABLES, targetTableName, String.class);
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
            return MatchUtils.getAllocateIdMatchConfigForAccount(customerSpace.toString(), matchInput, columnNames,
                    getSystemIds(BusinessEntity.Account), null);
        } else {
            return MatchUtils.getLegacyMatchConfigForAccount(customerSpace.toString(), matchInput, columnNames);
        }
    }

}
