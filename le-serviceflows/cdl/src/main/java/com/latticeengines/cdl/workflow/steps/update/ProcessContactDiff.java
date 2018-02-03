package com.latticeengines.cdl.workflow.steps.update;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessContactStepConfiguration;

@Component(ProcessContactDiff.BEAN_NAME)
public class ProcessContactDiff extends BaseProcessSingleEntityDiffStep<ProcessContactStepConfiguration> {

    static final String BEAN_NAME = "processContactDiff";

    @Override
    protected PipelineTransformationRequest getTransformRequest() {
        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName("ConsolidateContactDiff");
        TransformationStepConfig sort = sort(-1, 50);
        List<TransformationStepConfig> steps = new ArrayList<>();
        steps.add(sort);
        request.setSteps(steps);
        return request;
    }

    @Override
    protected TableRoleInCollection profileTableRole() {
        return null;
    }

}
