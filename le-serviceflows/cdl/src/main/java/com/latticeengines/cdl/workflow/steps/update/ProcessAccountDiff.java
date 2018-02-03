package com.latticeengines.cdl.workflow.steps.update;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;

@Component(ProcessAccountDiff.BEAN_NAME)
public class ProcessAccountDiff extends BaseProcessSingleEntityDiffStep<ProcessAccountStepConfiguration> {

    static final String BEAN_NAME = "processAccountDiff";

    @Override
    protected PipelineTransformationRequest getTransformRequest() {
        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName("ConsolidateAccountDiff");

        int matchStep = 0;
        int bucketStep = 1;
        int retainStep = 2;

        TransformationStepConfig matchDiff = match();
        TransformationStepConfig bucket = bucket(matchStep, true);
        TransformationStepConfig retainFields = retainFields(bucketStep, false);
        TransformationStepConfig sort = sort(retainStep, 200);

        List<TransformationStepConfig> steps = new ArrayList<>();
        steps.add(matchDiff);
        steps.add(bucket);
        steps.add(retainFields);
        steps.add(sort);
        request.setSteps(steps);
        return request;
    }

    @Override
    protected TableRoleInCollection profileTableRole() {
        return TableRoleInCollection.Profile;
    }
}
