package com.latticeengines.cdl.workflow.steps.merge;

import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessProductStepConfiguration;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.serviceflows.workflow.util.ETLEngineLoad;

@Component(MergeProductImports.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MergeProductImports extends BaseSingleEntityMergeImports<ProcessProductStepConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(MergeProductImports.class);

    static final String BEAN_NAME = "mergeProductImports";

    @Override
    public PipelineTransformationRequest getConsolidateRequest() {
        try {
            PipelineTransformationRequest request = new PipelineTransformationRequest();
            request.setName("MergeProductImports");
            TransformationStepConfig merge = mergeInputs(getConsolidateDataTxmfrConfig(false, true, true),
                    mergedBatchStoreName, ETLEngineLoad.LIGHT, null, -1);
            List<TransformationStepConfig> steps = Collections.singletonList(merge);
            request.setSteps(steps);
            return request;
        } catch (Exception e) {
            log.error("Failed to run consolidate data pipeline!", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void onPostTransformationCompleted() {
        putStringValueInContext(MERGED_PRODUCT_IMPORTS, TableUtils.getFullTableName(mergedBatchStoreName, pipelineVersion));
    }

}
