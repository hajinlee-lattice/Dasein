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
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.DynamoExportConfig;

@Component(SoftDeleteAccount.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class SoftDeleteAccount extends BaseSingleEntitySoftDelete<ProcessAccountStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(SoftDeleteAccount.class);

    static final String BEAN_NAME = "softDeleteAccount";

    @Override
    protected PipelineTransformationRequest getConsolidateRequest() {

        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName("SoftDeleteAccount");

        List<TransformationStepConfig> steps = new ArrayList<>();

        int softDeleteMergeStep = 0;
        TransformationStepConfig mergeSoftDelete = mergeSoftDelete(softDeleteActions);
        TransformationStepConfig softDelete = softDelete(softDeleteMergeStep);
        steps.add(mergeSoftDelete);
        steps.add(softDelete);

        request.setSteps(steps);

        return request;
    }

    @Override
    protected void onPostTransformationCompleted() {
        super.onPostTransformationCompleted();
        relinkDynamo();
    }

    private void relinkDynamo() {
        DynamoExportConfig config = new DynamoExportConfig();
        config.setTableName(getBatchStoreName());
        config.setLinkTableName(masterTable.getName());
        config.setRelink(Boolean.TRUE);
        addToListInContext(TABLES_GOING_TO_DYNAMO, config, DynamoExportConfig.class);
    }
}
