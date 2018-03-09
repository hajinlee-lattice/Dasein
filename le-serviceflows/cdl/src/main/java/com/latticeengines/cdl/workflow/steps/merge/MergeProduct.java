package com.latticeengines.cdl.workflow.steps.merge;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessProductStepConfiguration;

@Component(MergeProduct.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MergeProduct extends BaseSingleEntityMergeImports<ProcessProductStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(MergeProduct.class);

    static final String BEAN_NAME = "mergeProduct";

    private int mergeStep;
    private int upsertMasterStep;
    private int diffStep;

    public PipelineTransformationRequest getConsolidateRequest() {
        try {
            PipelineTransformationRequest request = new PipelineTransformationRequest();
            request.setName("MergeProduct");

            mergeStep = 0;
            upsertMasterStep = 1;
            diffStep = 2;

            TransformationStepConfig merge = mergeInputs(false, true, false);
            TransformationStepConfig upsertMaster = mergeMaster(mergeStep);
            TransformationStepConfig diff = diff(mergeStep, upsertMasterStep);
            TransformationStepConfig report = reportDiff(diffStep);

            List<TransformationStepConfig> steps = new ArrayList<>();
            steps.add(merge);
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

}
