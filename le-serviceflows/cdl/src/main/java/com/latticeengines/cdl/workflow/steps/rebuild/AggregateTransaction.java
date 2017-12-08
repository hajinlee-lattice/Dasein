package com.latticeengines.cdl.workflow.steps.rebuild;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessTransactionStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformWrapperStep;

@Component(AggregateTransaction.BEAN_NAME)
public class AggregateTransaction extends BaseTransformWrapperStep<ProcessTransactionStepConfiguration> {

    public static final String BEAN_NAME = "aggregateTransaction";

    private CustomerSpace customerSpace;

    int productAgrStep, periodedStep, dailyAgrStep, dayPeriodStep, periodAgrStep, periodsStep;

    private void initializeConfiguration() {
        customerSpace = configuration.getCustomerSpace();
    }

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        initializeConfiguration();

        PipelineTransformationRequest request = new PipelineTransformationRequest();
        request.setName("CalculatePurchaseHistory");
        request.setSubmitter(customerSpace.getTenantId());
        request.setKeepTemp(false);
        request.setEnableSlack(false);
        List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
        productAgrStep = 0;
        periodedStep = 1;
        dailyAgrStep = 2;
        dayPeriodStep = 3;
        periodAgrStep = 5;
        periodsStep = 6;
//        TransformationStepConfig productAgr  = rollupProduct(productMap);
//        TransformationStepConfig perioded = addPeriod();
//        TransformationStepConfig dailyAgr  = aggregateDaily();
//        TransformationStepConfig dayPeriods = collectDays();
//        TransformationStepConfig updateDaily  = updateDailyStore();
//        TransformationStepConfig periodAgr  = aggregatePeriods();
//        TransformationStepConfig periods  = collectPeriods();
//        TransformationStepConfig updatePeriod  = updatePeriodStore();
//        steps.add(productAgr);
//        steps.add(perioded);
//        steps.add(dailyAgr);
//        steps.add(dayPeriods);
//        steps.add(updateDaily);
//        steps.add(periodAgr);
//        steps.add(periods);
//        steps.add(updatePeriod);

        return transformationProxy.getWorkflowConf(request, configuration.getPodId());
    }

    @Override
    protected void onPostTransformationCompleted() {

    }

}
