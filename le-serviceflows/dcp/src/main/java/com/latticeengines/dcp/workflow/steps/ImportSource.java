package com.latticeengines.dcp.workflow.steps;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.eai.EaiJobConfiguration;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.eai.S3FileToHdfsConfiguration;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.dcp.steps.ImportSourceStepConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;
import com.latticeengines.proxy.exposed.dcp.SourceProxy;
import com.latticeengines.proxy.exposed.dcp.UploadProxy;
import com.latticeengines.proxy.exposed.eai.EaiProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("startImportSource")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ImportSource extends BaseWorkflowStep<ImportSourceStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ImportSource.class);

    @Inject
    private EaiProxy eaiProxy;

    @Inject
    private SourceProxy sourceProxy;

    @Inject
    private DataFeedProxy dataFeedProxy;

    @Inject
    private DropBoxProxy dropBoxProxy;

    @Inject
    private UploadProxy uploadProxy;

    @Override
    public void execute() {
        log.info("Start import DCP file");
        CustomerSpace customerSpace = configuration.getCustomerSpace();
        Upload upload = uploadProxy.getUpload(customerSpace.toString(), configuration.getUploadPid());
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTaskBySourceId(customerSpace.toString(),
                configuration.getSourceId());
        if (dataFeedTask == null) {
            throw new RuntimeException("Cannot find template for source " + configuration.getSourceId());
        }
        DropBoxSummary dropBoxSummary = dropBoxProxy.getDropBox(customerSpace.toString());
        importTable(dataFeedTask, dropBoxSummary, upload);
    }

    private void importTable(DataFeedTask dataFeedTask, DropBoxSummary dropBoxSummary,
                             Upload upload) {
        EaiJobConfiguration importConfig = setupConfiguration(dataFeedTask, dropBoxSummary, upload);
        AppSubmission submission = eaiProxy.submitEaiJob(importConfig);
        String applicationId = submission.getApplicationIds().get(0);
        dataFeedTask.setActiveJob(applicationId);
        dataFeedProxy.updateDataFeedTask(importConfig.getCustomerSpace().toString(), dataFeedTask, true);
        saveOutputValue(WorkflowContextConstants.Outputs.EAI_JOB_APPLICATION_ID, applicationId);
        waitForAppId(applicationId);
    }

    private S3FileToHdfsConfiguration setupConfiguration(DataFeedTask dataFeedTask, DropBoxSummary dropBoxSummary,
                                                         Upload upload) {
        S3FileToHdfsConfiguration s3FileToHdfsConfiguration = new S3FileToHdfsConfiguration();
        List<String> identifiers = new ArrayList<>();

        s3FileToHdfsConfiguration.setCustomerSpace(configuration.getCustomerSpace());
        s3FileToHdfsConfiguration.setS3Bucket(dropBoxSummary.getBucket());
        s3FileToHdfsConfiguration.setS3FilePath(upload.getUploadConfig().getUploadRawFilePath());
        s3FileToHdfsConfiguration.setBusinessEntity(BusinessEntity.getByName(dataFeedTask.getEntity()));
        s3FileToHdfsConfiguration.setJobIdentifier(dataFeedTask.getUniqueId());
        SourceImportConfiguration sourceImportConfig = new SourceImportConfiguration();
        sourceImportConfig.setSourceType(SourceType.FILE);
        s3FileToHdfsConfiguration.addSourceConfiguration(sourceImportConfig);

        identifiers.add(dataFeedTask.getUniqueId());
        s3FileToHdfsConfiguration.setProperty(ImportProperty.COLLECTION_IDENTIFIERS, JsonUtils.serialize(identifiers));

        return s3FileToHdfsConfiguration;
    }


}
