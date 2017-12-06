package com.latticeengines.cdl.workflow.steps.update;

import static com.latticeengines.cdl.workflow.steps.update.ProcessAccountDiff.BEAN_NAME;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;

@Component(BEAN_NAME)
public class ProcessAccountDiff extends BaseProcessSingleEntityDiffStep<ProcessAccountStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ProcessAccountDiff.class);

    static final String BEAN_NAME = "processAccountDiff";

    int matchStep;
    int bucketStep;
    int retainStep;

    @Override
    protected PipelineTransformationRequest getTransformRequest() {
        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName("ConsolidateAccountDiff");

        matchStep = 0;
        bucketStep = 1;
        retainStep = 2;

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
