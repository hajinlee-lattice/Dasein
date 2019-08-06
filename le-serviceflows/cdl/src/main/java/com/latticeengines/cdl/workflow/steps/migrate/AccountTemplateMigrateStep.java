package com.latticeengines.cdl.workflow.steps.migrate;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ImportMigrateTracking;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.EntityType;

@Component("accountTemplateMigrateStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class AccountTemplateMigrateStep extends BaseImportTemplateMigrateStep {

    @Override
    protected String getTemplateName() {
        return String.format(TEMPLATE_PATTERN, BusinessEntity.Account.name(), RandomStringUtils.randomAlphanumeric(8));
    }

    @Override
    protected String getEntity() {
        return BusinessEntity.Account.name();
    }

    @Override
    protected String getFeedType() {
        return String.format(FEEDTYPE_PATTERN, getSystemName(), EntityType.Accounts.getDefaultFeedTypeName());
    }

    @Override
    protected void updateTemplate(Table templateTable, S3ImportSystem s3ImportSystem) {
        Preconditions.checkNotNull(s3ImportSystem);
        Preconditions.checkNotNull(templateTable);
        String displayName = templateTable.getAttribute(InterfaceName.AccountId).getDisplayName();
        templateTable.removeAttribute(InterfaceName.AccountId.name());
        templateTable.addAttribute(getCustomerAccountId(displayName));
        if (StringUtils.isNotEmpty(s3ImportSystem.getAccountSystemId())) {
            templateTable.addAttribute(getSystemId(s3ImportSystem.getAccountSystemId(), displayName));
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
        importMigrateTracking.getReport().setOutputAccountTaskId(taskId);
        importMigrateTracking.getReport().setOutputAccountTemplate(templateName);
        migrateTrackingProxy.updateReport(customerSpace.toString(), importMigrateTracking.getPid(), importMigrateTracking.getReport());
    }
}
