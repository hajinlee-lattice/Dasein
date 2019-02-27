package com.latticeengines.cdl.workflow.steps.merge;

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
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ContactNameConcatenateConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessContactStepConfiguration;

@Component(MergeContact.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MergeContact extends BaseSingleEntityMergeImports<ProcessContactStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(MergeContact.class);

    static final String BEAN_NAME = "mergeContact";

    private int mergeStep;
    private int concatenateStep;
    private int upsertMasterStep;
    private int diffStep;

    public PipelineTransformationRequest getConsolidateRequest() {
        try {

            PipelineTransformationRequest request = new PipelineTransformationRequest();
            request.setName("MergeContact");

            mergeStep = 0;
            concatenateStep = 1;
            upsertMasterStep = 2;
            diffStep = 3;

            TransformationStepConfig merge = mergeInputs(false, true, false);
            TransformationStepConfig concatenate = concatenateContactName(mergeStep);
            TransformationStepConfig upsertMaster = mergeMaster(concatenateStep);
            TransformationStepConfig diff = diff(concatenateStep, upsertMasterStep);
            TransformationStepConfig report = reportDiff(diffStep);

            List<TransformationStepConfig> steps = new ArrayList<>();
            steps.add(merge);
            steps.add(concatenate);
            steps.add(upsertMaster);
            steps.add(diff);
            steps.add(report);
            request.setSteps(steps);
            return request;

        } catch (Exception e) {
            log.error("Failed to run consolidate data pipeline!", e);
            throw new RuntimeException(e);
        }
    }

    private TransformationStepConfig concatenateContactName(int mergeStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setInputSteps(Collections.singletonList(mergeStep));
        step.setTransformer(DataCloudConstants.TRANSFORMER_CONTACT_NAME_CONCATENATER);
        ContactNameConcatenateConfig config = new ContactNameConcatenateConfig();
        config.setConcatenateFields(new String[] { InterfaceName.FirstName.name(), InterfaceName.LastName.name() });
        config.setResultField(InterfaceName.ContactName.name());
        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));

        return step;
    }
}
