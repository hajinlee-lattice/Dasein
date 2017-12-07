package com.latticeengines.cdl.workflow.steps.update;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessContactStepConfiguration;

@Component(ProcessContactDiff.BEAN_NAME)
public class ProcessContactDiff extends BaseProcessSingleEntityDiffStep<ProcessContactStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ProcessContactDiff.class);

    static final String BEAN_NAME = "processContactDiff";

    int bucketStep;
    int retainStep;

    @Override
    protected PipelineTransformationRequest getTransformRequest() {
        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName("ConsolidateContactDiff");

        bucketStep = 0;
        retainStep = 1;

        TransformationStepConfig bucket = bucket(false);
        TransformationStepConfig retainFields = retainFields(bucketStep, false);
        TransformationStepConfig sort = sort(retainStep, 50);

        List<TransformationStepConfig> steps = new ArrayList<>();
        steps.add(bucket);
        steps.add(retainFields);
        steps.add(sort);
        request.setSteps(steps);
        return request;
    }

    @Override
    protected TableRoleInCollection profileTableRole() {
        return TableRoleInCollection.ContactProfile;
    }
}
