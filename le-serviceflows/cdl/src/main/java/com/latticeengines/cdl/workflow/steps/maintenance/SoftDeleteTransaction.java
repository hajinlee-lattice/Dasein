package com.latticeengines.cdl.workflow.steps.maintenance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.maintenance.SoftDeleteTransactionConfiguration;

@Component(SoftDeleteTransaction.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class SoftDeleteTransaction extends BaseSingleEntitySoftDelete<SoftDeleteTransactionConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(SoftDeleteTransaction.class);

    static final String BEAN_NAME = "softDeleteTransaction";

    @Override
    protected PipelineTransformationRequest getConsolidateRequest() {
        return null;
    }
}
