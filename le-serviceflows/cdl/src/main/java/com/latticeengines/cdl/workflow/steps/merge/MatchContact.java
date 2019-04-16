package com.latticeengines.cdl.workflow.steps.merge;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MATCH;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.config.atlas.ContactNameConcatenateConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessContactStepConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;

@Component(MatchContact.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MatchContact extends BaseSingleEntityMergeImports<ProcessContactStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(MatchContact.class);

    static final String BEAN_NAME = "matchContact";

    private String matchTargetTablePrefix = null;

    private int mergeStep;
    private int concatenateStep;
    private int matchStep;

    @Override
    public PipelineTransformationRequest getConsolidateRequest() {
        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName("MatchContact");
        matchTargetTablePrefix = entity.name() + "_Matched";

        boolean entityMatchEnabled = configuration.isEntityMatchEnabled();
        if (entityMatchEnabled) {
            request.setSteps(entityMatchSteps());
        } else {
            request.setSteps(legacyMatchSteps());
        }
        return request;
    }

    @Override
    protected void onPostTransformationCompleted() {
        String targetTableName = getEntityMatchTargetTableName();
        putStringValueInContext(ENTITY_MATCH_CONTACT_TARGETTABLE, targetTableName);
        addToListInContext(TEMPORARY_CDL_TABLES, targetTableName, String.class);
    }

    private List<TransformationStepConfig> entityMatchSteps() {
        List<TransformationStepConfig> steps = new ArrayList<>();
        mergeStep = 0;
        concatenateStep = 1;
        matchStep = 2;
        TransformationStepConfig merge = mergeInputs(false, false, true);
        TransformationStepConfig concatenate = concatenateContactName(mergeStep, null);
        TransformationStepConfig entityMatch = leadToAccountMatch(concatenateStep, matchTargetTablePrefix);
        steps.add(merge);
        steps.add(concatenate);
        steps.add(entityMatch);
        return steps;
    }

    private List<TransformationStepConfig> legacyMatchSteps() {
        List<TransformationStepConfig> steps = new ArrayList<>();
        mergeStep = 0;
        TransformationStepConfig merge = mergeInputs(false, true, false);
        TransformationStepConfig concatenate = concatenateContactName(mergeStep, matchTargetTablePrefix);
        steps.add(merge);
        steps.add(concatenate);
        return steps;
    }

    private TransformationStepConfig concatenateContactName(int mergeStep, String targetTableName) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(mergeStep));
        if (targetTableName != null) {
            setTargetTable(step, targetTableName);
        }
        step.setTransformer(DataCloudConstants.TRANSFORMER_CONTACT_NAME_CONCATENATER);
        ContactNameConcatenateConfig config = new ContactNameConcatenateConfig();
        config.setConcatenateFields(new String[] { InterfaceName.FirstName.name(), InterfaceName.LastName.name() });
        config.setResultField(InterfaceName.ContactName.name());
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));

        return step;
    }

    private TransformationStepConfig leadToAccountMatch(int inputStep, String targetTableName) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(inputStep));
        setTargetTable(step, targetTableName);
        step.setTransformer(TRANSFORMER_MATCH);
        String configStr = MatchUtils.getAllocateIdMatchConfigForContact(getBaseMatchInput(),
                getInputTableColumnNames(0));
        step.setConfiguration(configStr);
        return step;
    }

    private String getEntityMatchTargetTableName() {
        return TableUtils.getFullTableName(matchTargetTablePrefix, pipelineVersion);
    }

}
