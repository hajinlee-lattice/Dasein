package com.latticeengines.cdl.workflow.steps.validations;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessTransactionStepConfiguration;

@Lazy
@Component("validateTransactionBatchStore")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ValidateTransactionBatchStore extends BaseValidateReportBatchStore<ProcessTransactionStepConfiguration> {

}
