package com.latticeengines.cdl.workflow.steps.importdata;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.eai.EaiImportJobDetail;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.EntityValidationSummary;
import com.latticeengines.domain.exposed.pls.ProductValidationSummary;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata.ImportDataReportConfiguration;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.eai.EaiJobDetailProxy;
import com.latticeengines.serviceflows.workflow.report.BaseReportStep;

@Component("importDataReport")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ImportDataReport extends BaseReportStep<ImportDataReportConfiguration> {

    @Inject
    private EaiJobDetailProxy eaiJobDetailProxy;

    @Override
    public void execute() {
        String applicationId = getOutputValue(WorkflowContextConstants.Outputs.EAI_JOB_APPLICATION_ID);
        String tenantId = configuration.getCustomerSpace().getTenantId();

        EaiImportJobDetail eaiImportJobDetail = eaiJobDetailProxy.getImportJobDetailByAppId(applicationId);
        EntityValidationSummary entityValidationSummary = getObjectFromContext(ENTITY_VALIDATION_SUMMARY,
                EntityValidationSummary.class);

        long totalRows = eaiImportJobDetail.getTotalRows();
        BusinessEntity entity = configuration.getEntity();
        long errorLineInValidationStep = entityValidationSummary.getErrorLineNumber();
        // update eaiJobDetail if found error in validations
        if (errorLineInValidationStep != 0L) {
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
        }
        super.execute();
        failWorkflowIfNeeded(entity, errorLineInValidationStep, totalFailed, entityValidationSummary);
    }

    private void failWorkflowIfNeeded(BusinessEntity entity, long errorLineInValidationStep, long totalFailed,
                                      EntityValidationSummary entityValidationSummary) {
        if (errorLineInValidationStep != 0 && BusinessEntity.Product.equals(entity)) {
            ProductValidationSummary summary = (ProductValidationSummary) entityValidationSummary;
            String statistics = generateStatisticsForProduct(summary);
            String errorMessage = String.format("Import failed because there were %s errors : %s",
                    String.valueOf(totalFailed), statistics);
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

    @Override
    protected ReportPurpose getPurpose() {
        return ReportPurpose.IMPORT_DATA_SUMMARY;
    }
}
