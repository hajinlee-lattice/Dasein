package com.latticeengines.cdl.workflow.steps.maintenance;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessContactStepConfiguration;

@Component(SoftDeleteContact.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class SoftDeleteContact extends BaseSingleEntitySoftDelete<ProcessContactStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(SoftDeleteContact.class);

    static final String BEAN_NAME = "softDeleteContact";

    @Override
    protected PipelineTransformationRequest getConsolidateRequest() {
        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName("SoftDeleteContact");

        List<TransformationStepConfig> steps = new ArrayList<>();

        int softDeleteMergeStep = 0;
        TransformationStepConfig mergeSoftDelete = mergeSoftDelete(softDeleteActions);
        TransformationStepConfig softDelete = softDelete(softDeleteMergeStep);
        steps.add(mergeSoftDelete);
        steps.add(softDelete);

        request.setSteps(steps);

        return request;
    }
}
