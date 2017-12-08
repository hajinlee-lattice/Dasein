package com.latticeengines.cdl.workflow.steps.rebuild;


import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessTransactionStepConfiguration;

@Component(ProfilePurchaseHistory.BEAN_NAME)
public class ProfilePurchaseHistory extends BaseSingleEntityProfileStep<ProcessTransactionStepConfiguration> {

    static final String BEAN_NAME = "profilePurchaseHistory";

    @Override
    protected TableRoleInCollection profileTableRole() {
        return TableRoleInCollection.PurchaseHistoryProfile;
    }

    @Override
    protected PipelineTransformationRequest getTransformRequest() {
        String masterTableName = masterTable.getName();
        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName("ConsolidateProductStep");
        request.setSubmitter(customerSpace.getTenantId());
        request.setKeepTemp(false);
        request.setEnableSlack(false);
        // -----------
        List<TransformationStepConfig> steps = new ArrayList<>();
        // -----------
        request.setSteps(steps);
        return request;
    }

}
