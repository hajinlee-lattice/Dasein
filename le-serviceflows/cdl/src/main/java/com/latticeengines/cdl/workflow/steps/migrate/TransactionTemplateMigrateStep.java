package com.latticeengines.cdl.workflow.steps.migrate;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
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
        templateTable.removeAttribute(InterfaceName.ContactId.name());
        templateTable.addAttribute(getCustomerContactId());
        if (StringUtils.isNotEmpty(s3ImportSystem.getContactSystemId())) {
            templateTable.addAttribute(getSystemId(s3ImportSystem.getContactSystemId()));
        }
    }
}
