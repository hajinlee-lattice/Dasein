package com.latticeengines.cdl.workflow.steps.update;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessContactStepConfiguration;

@Component(ProcessContactDiff.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ProcessContactDiff extends BaseProcessSingleEntityDiffStep<ProcessContactStepConfiguration> {

    static final String BEAN_NAME = "processContactDiff";

    @Override
    protected PipelineTransformationRequest getTransformRequest() {
        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName("ConsolidateContactDiff");

        int bucketStep = 0;

        TransformationStepConfig bucket = bucket(false);
        TransformationStepConfig retainFields = retainFields(bucketStep);

        List<TransformationStepConfig> steps = new ArrayList<>();
        steps.add(bucket);
        steps.add(retainFields);
        request.setSteps(steps);
        return request;
    }

    @Override
    protected TableRoleInCollection profileTableRole() {
        return TableRoleInCollection.ContactProfile;
    }
}
