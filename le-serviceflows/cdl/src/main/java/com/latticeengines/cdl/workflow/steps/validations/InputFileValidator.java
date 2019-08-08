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
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.InputFileValidatorConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service.InputFileValidationConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service.impl.AccountFileValidationConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service.impl.ContactFileValidationConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service.impl.ProductFileValidationConfiguration;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.eai.EaiJobDetailProxy;
import com.latticeengines.serviceflows.workflow.report.BaseReportStep;

@Component("inputFileValidator")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class InputFileValidator extends BaseReportStep<InputFileValidatorConfiguration> {
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
        List<String> processedRecords = eaiImportJobDetail.getPRDetail();
        pathList = pathList == null ? null : pathList.stream().filter(StringUtils::isNotBlank).map(path -> {
            int index = path.indexOf("/Pods/");
            path = index > 0 ? path.substring(index) : path;
            return path;
        }).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(pathList) || CollectionUtils.isEmpty(processedRecords)
                || pathList.size() != processedRecords.size()) {
            log.warn(String.format("Avro path is empty for applicationId=%s, tenantId=%s", applicationId, tenantId));
            return;
        }

        BusinessEntity entity = configuration.getEntity();
        boolean enableEntityMatch = configuration.isEnableEntityMatch();
        log.info(String.format("Begin to validate data with entity %s and entity match %s.", entity.name(),
                String.valueOf(enableEntityMatch)));
        boolean enableEntityMatchGA = configuration.isEnableEntityMatchGA();
        // errorLine is used to count number of error found in this step
        long errorLine;
        InputFileValidationConfiguration fileConfiguration = generateConfiguration(entity, pathList,
                enableEntityMatch, enableEntityMatchGA);
        // this variable is used to generate statistics for product
        StringBuilder statistics = new StringBuilder();
        if (fileConfiguration == null) {
            log.info(String.format(
                    "skip validation as file configuration is null, the validation for this file with %s waiting to be implemented.",
                    entity));
            errorLine = 0L;
        } else {
            InputFileValidationService fileValidationService = InputFileValidationService
                    .getValidationService(fileConfiguration.getClass().getSimpleName());
            errorLine = fileValidationService.validate(fileConfiguration, processedRecords, statistics);
        }
        // update eaiJobDetail if found error in validations
        if (errorLine != 0L) {
            eaiImportJobDetail.setPRDetail(processedRecords);
            eaiImportJobDetail.setIgnoredRows(eaiImportJobDetail.getIgnoredRows() + errorLine);
            eaiImportJobDetail.setProcessedRecords(eaiImportJobDetail.getProcessedRecords() - (int) errorLine);
            eaiJobDetailProxy.updateImportJobDetail(eaiImportJobDetail);
        }

        // add report for this step and import data step
        if (errorLine != 0 && BusinessEntity.Product.equals(entity)) {
            getJson().put(entity.toString(), eaiImportJobDetail.getTotalRows())
                            .put("total_rows", eaiImportJobDetail.getTotalRows())
                            .put("ignored_rows", 0L)
                            .put("imported_rows", 0L)
                            .put("deduped_rows", 0L).put("total_failed_rows", eaiImportJobDetail.getTotalRows());
        } else {
            Long totalFailed = 0L;
            totalFailed += eaiImportJobDetail.getIgnoredRows() == null ? 0L : eaiImportJobDetail.getIgnoredRows();
            totalFailed += eaiImportJobDetail.getDedupedRows() == null ? 0L : eaiImportJobDetail.getDedupedRows();
            getJson().put(entity.toString(), eaiImportJobDetail.getProcessedRecords())
                    .put("total_rows", eaiImportJobDetail.getTotalRows())
                    .put("ignored_rows", eaiImportJobDetail.getIgnoredRows())
                    .put("imported_rows", eaiImportJobDetail.getProcessedRecords())
                    .put("deduped_rows", eaiImportJobDetail.getDedupedRows()).put("total_failed_rows", totalFailed);
        }
        super.execute();
        // make sure report first, then throw exception if necessary
        if (errorLine != 0 && BusinessEntity.Product.equals(entity)) {
            throw new LedpException(LedpCode.LEDP_40059, new String[] { statistics.toString(),
                    ImportProperty.ERROR_FILE });
        }
    }

    private InputFileValidationConfiguration generateConfiguration(BusinessEntity entity, List<String> pathList,
            boolean enableEntityMatch, boolean enableEntityMatchGA) {
        switch (entity) {
        case Account:
            AccountFileValidationConfiguration accountConfig = new AccountFileValidationConfiguration();
            accountConfig.setEntity(entity);
            accountConfig.setPathList(pathList);
            accountConfig.setEnableEntityMatch(enableEntityMatch);
            accountConfig.setEnableEntityMatchGA(enableEntityMatchGA);
            return accountConfig;
        case Contact:
            ContactFileValidationConfiguration contactConfig = new ContactFileValidationConfiguration();
            contactConfig.setEntity(entity);
            contactConfig.setPathList(pathList);
            contactConfig.setEnableEntityMatch(enableEntityMatch);
            contactConfig.setEnableEntityMatchGA(enableEntityMatchGA);
            return contactConfig;
        case Product:
            ProductFileValidationConfiguration productConfig = new ProductFileValidationConfiguration();
            productConfig.setCustomerSpace(configuration.getCustomerSpace());
            productConfig.setEntity(entity);
            productConfig.setPathList(pathList);
            productConfig.setEnableEntityMatchGA(enableEntityMatchGA);
            return productConfig;
        default:
            return null;
        }
    }

    @Override
    protected ReportPurpose getPurpose() {
        return ReportPurpose.IMPORT_DATA_SUMMARY;
    }

}
