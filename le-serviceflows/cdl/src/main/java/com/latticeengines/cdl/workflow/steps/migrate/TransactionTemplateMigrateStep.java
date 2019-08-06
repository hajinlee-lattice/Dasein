package com.latticeengines.cdl.workflow.steps.migrate;

import org.apache.commons.lang3.RandomStringUtils;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ImportMigrateTracking;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.EntityType;

@Component("transactionTemplateMigrateStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class TransactionTemplateMigrateStep extends BaseImportTemplateMigrateStep {
    @Override
    protected String getTemplateName() {
        return String.format(TEMPLATE_PATTERN, BusinessEntity.Transaction.name(), RandomStringUtils.randomAlphanumeric(8));
    }

    @Override
    protected String getEntity() {
        return BusinessEntity.Transaction.name();
    }

    @Override
    protected String getFeedType() {
        return String.format(FEEDTYPE_PATTERN, getSystemName(), EntityType.ProductPurchases.getDefaultFeedTypeName());
    }

    @Override
    protected void updateTemplate(Table templateTable, S3ImportSystem s3ImportSystem) {
        Preconditions.checkNotNull(s3ImportSystem);
        Preconditions.checkNotNull(templateTable);
        Attribute accountId = templateTable.getAttribute(InterfaceName.AccountId);
        if (accountId != null) {
            templateTable.removeAttribute(InterfaceName.AccountId.name());
            templateTable.addAttribute(getCustomerAccountId(accountId.getDisplayName()));
        }
        Attribute contactId = templateTable.getAttribute(InterfaceName.ContactId);
        if (contactId != null) {
            templateTable.removeAttribute(InterfaceName.ContactId.name());
            templateTable.addAttribute(getCustomerContactId(contactId.getDisplayName()));
        }
    }

    @Override
    protected void updateMigrateTracking(String taskId, String templateName) {
        CustomerSpace customerSpace = configuration.getCustomerSpace();
        ImportMigrateTracking importMigrateTracking = migrateTrackingProxy.getMigrateTracking(customerSpace.toString(),
                configuration.getMigrateTrackingPid());
        if (importMigrateTracking == null || importMigrateTracking.getReport() == null) {
            throw new RuntimeException("The MigrateTracking record is not properly created!");
        }
        importMigrateTracking.getReport().setOutputTransactionTaskId(taskId);
        importMigrateTracking.getReport().setOutputTransactionTemplate(templateName);
        migrateTrackingProxy.updateReport(customerSpace.toString(), importMigrateTracking.getPid(), importMigrateTracking.getReport());
    }
}
