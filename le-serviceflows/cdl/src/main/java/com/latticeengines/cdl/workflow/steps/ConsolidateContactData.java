package com.latticeengines.cdl.workflow.steps;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateDataTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.ConsolidateContactDataStepConfiguration;

@Component("consolidateContactData")
public class ConsolidateContactData extends ConsolidateDataBase<ConsolidateContactDataStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ConsolidateContactData.class);

    private int mergeStep;
    private int upsertMasterStep;
    private int diffStep;

    @Override
    protected void initializeConfiguration() {
        super.initializeConfiguration();
        srcIdField = configuration.getIdField();
    }

    public PipelineTransformationRequest getConsolidateRequest() {
        try {

            PipelineTransformationRequest request = new PipelineTransformationRequest();
            request.setName("ConsolidatePipeline");

            mergeStep = 0;
            upsertMasterStep = 1;
            diffStep = 2;
            TransformationStepConfig merge = mergeInputs(false);
            TransformationStepConfig upsertMaster = mergeMaster(mergeStep);
            TransformationStepConfig diff = diff(mergeStep, upsertMasterStep);
            TransformationStepConfig sort = sortDiff(diffStep);

            List<TransformationStepConfig> steps = new ArrayList<>();
            steps.add(merge);
            steps.add(upsertMaster);
            if (isBucketing()) {
                steps.add(diff);
                steps.add(sort);
            }
            request.setSteps(steps);
            return request;

        } catch (Exception e) {
            log.error("Failed to run consolidate data pipeline!", e);
            throw new RuntimeException(e);
        }
    }


    @Override
    protected void setupConfig(ConsolidateDataTransformerConfig config) {
        config.setMasterIdField(TableRoleInCollection.ConsolidatedContact.getPrimaryKey().name());
    }

}
