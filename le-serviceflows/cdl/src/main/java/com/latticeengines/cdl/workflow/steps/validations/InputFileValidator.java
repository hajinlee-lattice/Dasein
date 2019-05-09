package com.latticeengines.cdl.workflow.steps.validations;

import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.validations.service.InputFileValidationService;
import com.latticeengines.domain.exposed.eai.EaiImportJobDetail;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.InputFileValidatorConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service.InputFileValidationConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service.impl.AccountFileValidationConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service.impl.ContactFileValidationConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service.impl.ProductFileValidationConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.eai.EaiJobDetailProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("inputFileValidator")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class InputFileValidator extends BaseWorkflowStep<InputFileValidatorConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(InputFileValidator.class);


    @Inject
    private EaiJobDetailProxy eaiJobDetailProxy;

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void execute() {

        String applicationId = getOutputValue(WorkflowContextConstants.Outputs.EAI_JOB_APPLICATION_ID);
        String tenantId = configuration.getCustomerSpace().getTenantId();

        if (applicationId == null) {
            log.warn("There's no application Id! tenentId=" + tenantId);
            return;
        }
        EaiImportJobDetail eaiImportJobDetail = eaiJobDetailProxy.getImportJobDetailByAppId(applicationId);
        if (eaiImportJobDetail == null) {
            log.warn(String.format("Cannot find the job detail for applicationId=%s, tenantId=%s", applicationId,
                    tenantId));
            return;
        }
        List<String> pathList = eaiImportJobDetail.getPathDetail();
        pathList = pathList == null ? null : pathList.stream().filter(StringUtils::isNotBlank).map(path -> {
            int index = path.indexOf("/Pods/");
            path = index > 0 ? path.substring(index) : path;
            return path;
        }).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(pathList)) {
            log.warn(String.format("Avro path is empty for applicationId=%s, tenantId=%s", applicationId, tenantId));
            return;
        }

        BusinessEntity entity = configuration.getEntity();
        log.info(String.format("Begin to validate data with entity %s.", entity.name()));
        InputFileValidationConfiguration fileConfiguration = generateConfiguration(entity, pathList);
        InputFileValidationService fileValidationService = InputFileValidationService
                .getValidationService(fileConfiguration.getClass().getSimpleName());
        fileValidationService.validate(fileConfiguration);

    }

    private InputFileValidationConfiguration generateConfiguration(BusinessEntity entity, List<String> pathList) {
        switch (entity) {
        case Account:
            AccountFileValidationConfiguration accountConfig = new AccountFileValidationConfiguration();
            accountConfig.setEntity(entity);
            accountConfig.setPathList(pathList);
            return accountConfig;
        case Contact:
            ContactFileValidationConfiguration contactConfig = new ContactFileValidationConfiguration();
            contactConfig.setEntity(entity);
            contactConfig.setPathList(pathList);
            return contactConfig;
        case Product:
            ProductFileValidationConfiguration productConfig = new ProductFileValidationConfiguration();
            productConfig.setCustomerSpace(configuration.getCustomerSpace());
            productConfig.setEntity(entity);
            productConfig.setPathList(pathList);
            return productConfig;
        default:
            throw new RuntimeException("File don't support validation");
        }
    }
}
