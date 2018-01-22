package com.latticeengines.cdl.workflow.steps.maintenance;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.eai.DeleteFileToHdfsConfiguration;
import com.latticeengines.domain.exposed.eai.EaiImportJobDetail;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.maintenance.DeleteFileUploadStepConfiguration;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.proxy.exposed.eai.EaiJobDetailProxy;
import com.latticeengines.proxy.exposed.eai.EaiProxy;
import com.latticeengines.serviceflows.workflow.report.BaseReportStep;

@Component("deleteFileUploadStep")
public class DeleteFileUploadStep extends BaseReportStep<DeleteFileUploadStepConfiguration> {

    @Autowired
    private EaiProxy eaiProxy;

    @Autowired
    private EaiJobDetailProxy eaiJobDetailProxy;

    @Override
    protected ReportPurpose getPurpose() {
        return ReportPurpose.MAINTENANCE_OPERATION_SUMMARY;
    }

    @Override
    public void execute() {
        AppSubmission submission = eaiProxy.submitEaiJob(generateImportConfiguration());
        String applicationId = submission.getApplicationIds().get(0);
        waitForAppId(applicationId);
        EaiImportJobDetail jobDetail = eaiJobDetailProxy.getImportJobDetailByAppId(applicationId);
        if (jobDetail != null) {
            getJson().put("tatal_rows", jobDetail.getTotalRows())
                    .put("imported_rows", jobDetail.getProcessedRecords())
                    .put("ignored_rows", jobDetail.getIgnoredRows())
                    .put("deduped_rows", jobDetail.getDedupedRows());
        } else {
            log.error("Cannot get Eai Import job detail for appId: " + applicationId);
        }
        super.execute();
    }

    private ImportConfiguration generateImportConfiguration() {
        DeleteFileToHdfsConfiguration importConfig = new DeleteFileToHdfsConfiguration();
        importConfig.setTableName(configuration.getTableName());
        importConfig.setFilePath(configuration.getFilePath());
        importConfig.setCustomerSpace(configuration.getCustomerSpace());
        List<String> identifiers = new ArrayList<>();
        identifiers.add(NamingUtils.uuid("DeleteFile"));
        importConfig.setProperty(ImportProperty.COLLECTION_IDENTIFIERS, JsonUtils.serialize(identifiers));
        SourceImportConfiguration sourceImportConfiguration = new SourceImportConfiguration();
        sourceImportConfiguration.setSourceType(SourceType.FILE);
        importConfig.addSourceConfiguration(sourceImportConfiguration);
        return importConfig;
    }
}
