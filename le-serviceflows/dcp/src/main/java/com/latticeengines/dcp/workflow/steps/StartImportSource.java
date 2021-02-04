package com.latticeengines.dcp.workflow.steps;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;

import javax.inject.Inject;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.io.ByteOrderMark;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.csv.LECSVFormat;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.dcp.DownloadFileType;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadDetails;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
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
        String uploadId = configuration.getUploadId();
        uploadProxy.updateUploadStatus(customerSpace.toString(), uploadId, Upload.Status.IMPORT_STARTED, null);
        UploadDetails uploadDetails = uploadProxy.getUploadByUploadId(customerSpace.toString(), uploadId, Boolean.TRUE);
        if (uploadDetails == null || uploadDetails.getUploadConfig() == null ||
                StringUtils.isEmpty(uploadDetails.getUploadConfig().getDropFilePath())) {
            throw new RuntimeException("Cannot start DCP import job due to lack of import info!");
        }

        String sourceKey = uploadDetails.getUploadConfig().getDropFilePath();
        String csvFileName = sourceKey.substring(sourceKey.lastIndexOf("/") + 1);

        // Build upload dir
        Source source = sourceProxy.getSource(customerSpace.toString(), configuration.getSourceId());
        ProjectDetails projectDetails = projectProxy.getDCPProjectByProjectId(customerSpace.toString(),
                configuration.getProjectId(), Boolean.FALSE, null);
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
                source.getSourceId(), uploadDetails.getDisplayName(), uploadTS);
        s3Service.createFolder(dropBoxSummary.getBucket(),
                UploadS3PathBuilderUtils.combinePath(false, false, dropFolder, uploadRaw));

        String uploadImportResult = UploadS3PathBuilderUtils.getUploadImportResultDir(projectDetails.getProjectId(),
                source.getSourceId(), uploadDetails.getDisplayName(), uploadTS);

        s3Service.createFolder(dropBoxSummary.getBucket(),
                UploadS3PathBuilderUtils.combinePath(false, false, dropFolder, uploadImportResult));

        uploadDetails.getUploadConfig().setUploadTimestamp(uploadTS);

        uploadDetails.getUploadConfig().setUploadRawFilePath(UploadS3PathBuilderUtils.combinePath(false, false, dropFolder,
                uploadRaw, csvFileName));
        uploadDetails.getUploadConfig().setDownloadableFiles(EnumSet.of(DownloadFileType.RAW));

        // Copy file from drop folder to raw input folder.
        copyFromDropfolder(uploadDetails, dropBoxSummary);

        long dropFileTime = 0;
        if (Boolean.TRUE.equals(uploadDetails.getUploadConfig().getSourceOnHdfs())) {
            try {
                FileStatus hdfsFile = HdfsUtils.getFileStatus(yarnConfiguration, sourceKey);
                dropFileTime = hdfsFile.getModificationTime();
            } catch (IOException e) {
                log.warn("Can not get time stamp of file " + sourceKey + " error="
                        + e.getMessage());
            }
        } else {
            ObjectMetadata sourceFile = s3Service.getObjectMetadata(dropBoxSummary.getBucket(), sourceKey);
            dropFileTime = sourceFile.getLastModified().getTime();
        }

        uploadProxy.updateUploadConfig(customerSpace.toString(), uploadId, uploadDetails.getUploadConfig());
        uploadProxy.updateDropFileTime(customerSpace.toString(), uploadId, dropFileTime);
        checkCSVFile(uploadDetails, dropBoxSummary);
    }

    private void copyFromDropfolder(UploadDetails upload, DropBoxSummary dropBoxSummary) {
        String sourceKey = upload.getUploadConfig().getDropFilePath();
        if (Boolean.TRUE.equals(upload.getUploadConfig().getSourceOnHdfs())) {
            RetryTemplate retry = RetryUtils.getRetryTemplate(10, //
                    Arrays.asList(AmazonS3Exception.class, RuntimeException.class), null);

            retry.execute(context -> {
                if (context.getRetryCount() > 0) {
                    log.info(String.format("(Attempt=%d) Retry copying object from hdfs:%s to %s:%s", //
                            context.getRetryCount() + 1, sourceKey,
                            dropBoxSummary.getBucket(), upload.getUploadConfig().getUploadRawFilePath()));
                }
                try (InputStream inputStream = HdfsUtils.getInputStream(yarnConfiguration, sourceKey)) {
                    s3Service.uploadInputStream(dropBoxSummary.getBucket(), upload.getUploadConfig().getUploadRawFilePath(),
                            inputStream, true);
                    return true;
                } catch (IOException e) {
                    throw new RuntimeException("Cannot get inputstream for hdfs file: " + sourceKey);
                }
            });
        } else {
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
        }
    }

    private void checkCSVFile(UploadDetails upload, DropBoxSummary dropBoxSummary) {
        if (StringUtils.isEmpty(upload.getUploadConfig().getUploadRawFilePath())) {
            throw new LedpException(LedpCode.LEDP_60004, new String[]{upload.getUploadId()});
        }
        if (!s3Service.objectExist(dropBoxSummary.getBucket(), upload.getUploadConfig().getUploadRawFilePath())) {
            throw new LedpException(LedpCode.LEDP_60005,
                    new String[]{dropBoxSummary.getBucket(), upload.getUploadConfig().getUploadRawFilePath()});
        }
        try (InputStream inputStream = s3Service.readObjectAsStream(dropBoxSummary.getBucket(),
                upload.getUploadConfig().getUploadRawFilePath())) {
            InputStreamReader reader = new InputStreamReader(
                    new BOMInputStream(inputStream, false, ByteOrderMark.UTF_8, ByteOrderMark.UTF_16LE,
                            ByteOrderMark.UTF_16BE, ByteOrderMark.UTF_32LE, ByteOrderMark.UTF_32BE),
                    StandardCharsets.UTF_8);
            CSVParser parser = new CSVParser(reader, LECSVFormat.format);
            if (!parser.iterator().hasNext()) {
                throw new LedpException(LedpCode.LEDP_60006, new String[]{upload.getUploadConfig().getUploadRawFilePath()});
            }
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_60007,
                    new String[]{dropBoxSummary.getBucket(), upload.getUploadConfig().getUploadRawFilePath()});
        } catch (IllegalArgumentException e) {
            throw new LedpException(LedpCode.LEDP_60008, new String[]{e.getMessage()});
        }
    }

}
