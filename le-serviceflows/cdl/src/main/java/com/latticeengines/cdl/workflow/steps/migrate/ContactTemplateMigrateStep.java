package com.latticeengines.cdl.workflow.steps.migrate;

import org.apache.commons.lang3.RandomStringUtils;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.MigrateTracking;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.EntityType;

@Component("contactTemplateMigrateStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ContactTemplateMigrateStep extends BaseImportTemplateMigrateStep {
    @Override
    protected String getTemplateName() {
        return String.format(TEMPLATE_PATTERN, BusinessEntity.Contact.name(), RandomStringUtils.randomAlphanumeric(8));
    }

    @Override
    protected String getEntity() {
        return BusinessEntity.Contact.name();
    }

    @Override
    protected String getFeedType() {
        return String.format(FEEDTYPE_PATTERN, getSystemName(), EntityType.Contacts.getDefaultFeedTypeName());
    }

    @Override
    protected void updateTemplate(Table templateTable, S3ImportSystem s3ImportSystem) {
        Preconditions.checkNotNull(s3ImportSystem);
        Preconditions.checkNotNull(templateTable);
        if (templateTable.getAttribute(InterfaceName.AccountId) != null) {
            templateTable.removeAttribute(InterfaceName.AccountId.name());
            templateTable.addAttribute(getCustomerAccountId());
        }
        if (templateTable.getAttribute(InterfaceName.ContactId) != null) {
            templateTable.removeAttribute(InterfaceName.ContactId.name());
            templateTable.addAttribute(getCustomerContactId());
        }
    }

    @Override
    protected void updateMigrateTracking(String taskId, String templateName) {
        CustomerSpace customerSpace = configuration.getCustomerSpace();
        MigrateTracking migrateTracking = migrateTrackingProxy.getMigrateTracking(customerSpace.toString(),
                configuration.getMigrateTrackingPid());
        if (migrateTracking == null || migrateTracking.getReport() == null) {
            throw new RuntimeException("The MigrateTracking record is not properly created!");
        }
        migrateTracking.getReport().setOutputContactTaskId(taskId);
        migrateTracking.getReport().setOutputContactTemplate(templateName);
        migrateTrackingProxy.updateReport(customerSpace.toString(), migrateTracking.getPid(), migrateTracking.getReport());
    }
}
