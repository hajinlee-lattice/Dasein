package com.latticeengines.cdl.workflow.steps.validations;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessContactStepConfiguration;

@Lazy
@Component("validateContactBatchStore")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ValidateContactBatchStore extends BaseValidateReportBatchStore<ProcessContactStepConfiguration> {

    @Override
    protected String getEntityContextKey() {
        return CONTACT_CHANGELIST_TABLE_NAME;
    }

    @Override
    protected String getEntityKey() {
        return InterfaceName.ContactId.name();
    }
}
