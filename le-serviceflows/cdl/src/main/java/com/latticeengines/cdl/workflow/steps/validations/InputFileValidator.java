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
import com.latticeengines.domain.exposed.pls.EntityValidationSummary;
import com.latticeengines.domain.exposed.pls.ProductValidationSummary;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.InputFileValidatorConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service.InputFileValidationConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service.impl.AccountFileValidationConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations.service.impl.CatalogFileValidationConfiguration;
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

    public static final long CATALOG_RECORDS_LIMIT = 10L;

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
        long totalRows = eaiImportJobDetail.getTotalRows();
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
        // generate summary for validation step
        EntityValidationSummary entityValidationSummary = null;
        long errorLineInValidationStep;
        InputFileValidationConfiguration fileConfiguration = generateConfiguration(entity, pathList,
                enableEntityMatch, enableEntityMatchGA, totalRows);
        if (fileConfiguration == null) {
            log.info(String.format(
                    "skip validation as file configuration is null, the validation for this file with %s waiting to be implemented.",
                    entity));
            errorLineInValidationStep = 0L;
        } else {
            InputFileValidationService fileValidationService = InputFileValidationService
                    .getValidationService(fileConfiguration.getClass());
            entityValidationSummary = fileValidationService.validate(fileConfiguration, processedRecords);
            errorLineInValidationStep = entityValidationSummary.getErrorLineNumber();
        }
        // update eaiJobDetail if found error in validations
        if (errorLineInValidationStep != 0L) {
            eaiImportJobDetail.setPRDetail(processedRecords);
            eaiImportJobDetail.setIgnoredRows(eaiImportJobDetail.getIgnoredRows() + errorLineInValidationStep);
            eaiImportJobDetail.setProcessedRecords(eaiImportJobDetail.getProcessedRecords() - (int) errorLineInValidationStep);
            eaiJobDetailProxy.updateImportJobDetail(eaiImportJobDetail);
        }

        // add report for this step and import data step
        long totalFailed = 0L;
        if (errorLineInValidationStep != 0 && BusinessEntity.Product.equals(entity)) {
            totalFailed += eaiImportJobDetail.getIgnoredRows() == null ? 0L : eaiImportJobDetail.getIgnoredRows();
            totalFailed += eaiImportJobDetail.getDedupedRows() == null ? 0L : eaiImportJobDetail.getDedupedRows();
            getJson().put(entity.toString(), totalRows)
                            .put("total_rows", totalRows)
                            .put("ignored_rows", 0L)
                            .put("imported_rows", 0L)
                            .put("deduped_rows", 0L)
                            .putPOJO("product_summary", entityValidationSummary).put("total_failed_rows",
                    totalFailed);
        } else if (BusinessEntity.Catalog.equals(entity) && eaiImportJobDetail.getTotalRows() > CATALOG_RECORDS_LIMIT) {
            getJson().put(entity.toString(), totalRows)
                    .put("total_rows", totalRows)
                    .put("ignored_rows", 0L)
                    .put("imported_rows", 0L)
                    .put("deduped_rows", 0L)
                    .put("total_failed_rows", totalRows);
        } else {
            totalFailed += eaiImportJobDetail.getIgnoredRows() == null ? 0L : eaiImportJobDetail.getIgnoredRows();
            totalFailed += eaiImportJobDetail.getDedupedRows() == null ? 0L : eaiImportJobDetail.getDedupedRows();
            getJson().put(entity.toString(), eaiImportJobDetail.getProcessedRecords())
                    .put("total_rows", totalRows)
                    .put("ignored_rows", eaiImportJobDetail.getIgnoredRows())
                    .put("imported_rows", eaiImportJobDetail.getProcessedRecords())
                    .put("deduped_rows", eaiImportJobDetail.getDedupedRows()).put("total_failed_rows", totalFailed);
        }
        super.execute();
        // make sure report first, then fail work flow if necessary
        failWorkflowIfNeeded(entity, errorLineInValidationStep, totalFailed, totalRows, entityValidationSummary);
    }

    private void failWorkflowIfNeeded(BusinessEntity entity, long errorLineInValidationStep, long totalFailed,
                                      long totalRows, EntityValidationSummary entityValidationSummary) {
        if (errorLineInValidationStep != 0 && BusinessEntity.Product.equals(entity)) {
            ProductValidationSummary summary = (ProductValidationSummary) entityValidationSummary;
            String statistics = generateStatisticsForProduct(summary);
            String errorMessage = String.format("Import failed because there were %s errors : %s",
                    String.valueOf(totalFailed), statistics);
            throw new LedpException(LedpCode.LEDP_40059, new String[] { errorMessage,
                    ImportProperty.ERROR_FILE });
        }
        if (totalRows > CATALOG_RECORDS_LIMIT && BusinessEntity.Catalog.equals(entity)) {
            String errorMessage = String.format("%s exceeds platform Limit - Please retry with no more than 10 rows.",
                    String.valueOf(totalRows));
            throw new LedpException(LedpCode.LEDP_40059, new String[] { errorMessage,
                    ImportProperty.ERROR_FILE });
        }
    }

    private String generateStatisticsForProduct(ProductValidationSummary summary) {
        int missingBundleInUse = summary.getMissingBundleInUse(), bundleWithDiffSku = summary.getDifferentSKU();
        StringBuilder statistics = new StringBuilder();
        if (missingBundleInUse != 0) {
            statistics.append(String.format("%s missing product bundles in use (this import will " +
                    "completely replace the previous one), ", String.valueOf(missingBundleInUse)));
        }
        if (bundleWithDiffSku != 0) {
            statistics.append(String.format("%s product bundle has different product SKUs. Dependant models will " +
                    "need to be remodelled to get accurate scores.", String.valueOf(bundleWithDiffSku)));
        }
        return statistics.toString();
    }

    private InputFileValidationConfiguration generateConfiguration(BusinessEntity entity, List<String> pathList,
            boolean enableEntityMatch, boolean enableEntityMatchGA, long totalRows) {
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
            productConfig.setDataFeedTaskId(configuration.getDataFeedTaskId());
            return productConfig;
        case Catalog:
            CatalogFileValidationConfiguration catalogConfig = new CatalogFileValidationConfiguration();
            catalogConfig.setCustomerSpace(configuration.getCustomerSpace());
            catalogConfig.setEnableEntityMatchGA(enableEntityMatchGA);
            catalogConfig.setEntity(entity);
            catalogConfig.setPathList(pathList);
            catalogConfig.setTotalRows(totalRows);
            return catalogConfig;
        default:
            return null;
        }
    }

    @Override
    protected ReportPurpose getPurpose() {
        return ReportPurpose.IMPORT_DATA_SUMMARY;
    }

}
