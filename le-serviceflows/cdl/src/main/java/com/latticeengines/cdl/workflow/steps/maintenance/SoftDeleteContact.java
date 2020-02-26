package com.latticeengines.cdl.workflow.steps.maintenance;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_SELECT_BY_COLUMN_TXFMR;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessContactStepConfiguration;
import com.latticeengines.domain.exposed.spark.cdl.SelectByColumnConfig;

@Lazy
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
        int selectContactStep = softDeleteMergeStep + 1;
        TransformationStepConfig mergeSoftDelete = mergeSoftDelete(softDeleteActions);
        TransformationStepConfig selectContact = selectContact(softDeleteMergeStep);
        TransformationStepConfig softDeleteSystemBatchStore = softDeleteSystemBatchStoreByContact(selectContactStep);
        TransformationStepConfig softDelete = softDelete(softDeleteMergeStep);
        steps.add(mergeSoftDelete);
        steps.add(selectContact);
        steps.add(softDeleteSystemBatchStore);
        steps.add(softDelete);

        request.setSteps(steps);

        return request;
    }

    private TransformationStepConfig selectContact(int mergeSoftDeleteStep) {
        TransformationStepConfig step = new TransformationStepConfig();
        step.setTransformer(TRANSFORMER_SELECT_BY_COLUMN_TXFMR);
        step.setInputSteps(Collections.singletonList(mergeSoftDeleteStep));
        if (masterTable != null) {
            log.info("Add masterTable=" + masterTable.getName());
            addBaseTables(step, masterTable.getName());
        } else {
            throw new IllegalArgumentException("The master table is empty for contact soft delete!");
        }

        SelectByColumnConfig config = new SelectByColumnConfig();
        config.setSourceColumn(InterfaceName.AccountId.name());
        config.setDestColumn(InterfaceName.ContactId.name());

        step.setConfiguration(appendEngineConf(config, lightEngineConfig()));
        return step;
    }

    @Override
    protected boolean skipRegisterBatchStore() {
        return false;
    }

    @Override
    protected boolean processSystemBatchStore() {
        return true;
    }
}
