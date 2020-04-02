package com.latticeengines.dcp.workflow.steps;

import java.util.Collections;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.serviceflows.dcp.steps.ImportSourceStepConfiguration;
import com.latticeengines.domain.exposed.util.UploadS3PathBuilderUtils;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;
import com.latticeengines.proxy.exposed.dcp.ProjectProxy;
import com.latticeengines.proxy.exposed.dcp.SourceProxy;
import com.latticeengines.proxy.exposed.dcp.UploadProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("startImportSource")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class StartImportSource extends BaseWorkflowStep<ImportSourceStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(StartImportSource.class);

    @Inject
    private UploadProxy uploadProxy;

    @Inject
    private SourceProxy sourceProxy;

    @Inject
    private ProjectProxy projectProxy;

    @Inject
    private S3Service s3Service;

    @Inject
    private DropBoxProxy dropBoxProxy;

    @Override
    public void execute() {
        CustomerSpace customerSpace = configuration.getCustomerSpace();
        Long uploadPid = configuration.getUploadPid();
        uploadProxy.updateUploadStatus(customerSpace.toString(), uploadPid, Upload.Status.IMPORT_STARTED);
        Upload upload = uploadProxy.getUpload(customerSpace.toString(), uploadPid);
        if (upload == null || upload.getUploadConfig() == null || StringUtils.isEmpty(upload.getUploadConfig().getDropFilePath())) {
            throw new RuntimeException("Cannot start DCP import job due to lack of import info!");
        }
        String sourceKey = upload.getUploadConfig().getDropFilePath();
        String csvFileName = sourceKey.substring(sourceKey.lastIndexOf("/") + 1);
        // Build upload dir
        Source source = sourceProxy.getSource(customerSpace.toString(), configuration.getSourceId());
        ProjectDetails projectDetails = projectProxy.getDCPProjectByProjectId(customerSpace.toString(),
                configuration.getProjectId());
        DropBoxSummary dropBoxSummary = dropBoxProxy.getDropBox(customerSpace.toString());
        String dropFolder = UploadS3PathBuilderUtils.getDropFolder(dropBoxSummary.getDropBox());
        String uploadDir = UploadS3PathBuilderUtils.getUploadRoot(projectDetails.getProjectId(), source.getSourceId());
        String uploadDirKey = UploadS3PathBuilderUtils.combinePath(false, true, dropFolder, uploadDir);
        if (!s3Service.objectExist(dropBoxSummary.getBucket(), uploadDirKey)) {
            s3Service.createFolder(dropBoxSummary.getBucket(), uploadDirKey);
        }
        String uploadTS;
        String currentUploadRoot;

        do {
            uploadTS = UploadS3PathBuilderUtils.getTimestampString();
            currentUploadRoot = UploadS3PathBuilderUtils.combinePath(false, true, dropFolder, uploadDir, uploadTS);
        } while (s3Service.objectExist(dropBoxSummary.getBucket(), currentUploadRoot));

        s3Service.createFolder(dropBoxSummary.getBucket(), currentUploadRoot);

        String uploadRaw = UploadS3PathBuilderUtils.getUploadRawDir(projectDetails.getProjectId(),
                source.getSourceId(), uploadTS);
        s3Service.createFolder(dropBoxSummary.getBucket(),
                UploadS3PathBuilderUtils.combinePath(false, false, dropFolder, uploadRaw));

        String uploadImportResult = UploadS3PathBuilderUtils.getUploadImportResultDir(projectDetails.getProjectId(),
                source.getSourceId(), uploadTS);
        s3Service.createFolder(dropBoxSummary.getBucket(),
                UploadS3PathBuilderUtils.combinePath(false, false, dropFolder, uploadImportResult));

        upload.getUploadConfig().setUploadTSPrefix(uploadTS);

        upload.getUploadConfig().setUploadRawFilePath(UploadS3PathBuilderUtils.combinePath(false, false, dropFolder,
                uploadRaw, csvFileName));

        // Copy file from drop folder to raw input folder.
        RetryTemplate retry = RetryUtils.getRetryTemplate(10, //
                Collections.singleton(AmazonS3Exception.class), null);
        retry.execute(context -> {
            if (context.getRetryCount() > 0) {
                log.info(String.format("(Attempt=%d) Retry copying object from %s:%s to %s:%s", //
                        context.getRetryCount() + 1, dropBoxSummary.getBucket(), sourceKey,
                        dropBoxSummary.getBucket(), upload.getUploadConfig().getUploadRawFilePath()));
            }
            s3Service.copyLargeObjects(dropBoxSummary.getBucket(), sourceKey, dropBoxSummary.getBucket(), upload.getUploadConfig().getUploadRawFilePath());
            return true;
        });

        uploadProxy.updateUploadConfig(customerSpace.toString(), uploadPid, upload.getUploadConfig());
    }
}
