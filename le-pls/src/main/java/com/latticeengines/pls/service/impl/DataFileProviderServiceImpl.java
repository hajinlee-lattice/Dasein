package com.latticeengines.pls.service.impl;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.app.exposed.download.CustomerSpaceHdfsFileDownloader;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ProvenancePropertyName;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.entitymanager.SourceFileEntityMgr;
import com.latticeengines.pls.service.DataFileProviderService;
import com.latticeengines.pls.service.impl.HdfsFileHttpDownloader.DownloadRequestBuilder;

@Component("dataFileProviderService")
public class DataFileProviderServiceImpl implements DataFileProviderService {

    private static final Logger log = LoggerFactory.getLogger(DataFileProviderServiceImpl.class);

    private static String MODEL_PROFILE_AVRO = "model_profile.avro";

    @Value("${pls.modelingservice.basedir}")
    private String modelingServiceHdfsBaseDir;

    @Autowired
    private Configuration yarnConfiguration;
    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Autowired
    private SourceFileEntityMgr sourceFileEntityMgr;

    @Override
    public void downloadFile(HttpServletRequest request, HttpServletResponse response, String modelId, String mimeType,
            String filter) throws IOException {

        HdfsFileHttpDownloader downloader = getDownloader(modelId, mimeType, filter);
        downloader.downloadFile(request, response);
    }

    @Override
    public void downloadPivotFile(HttpServletRequest request, HttpServletResponse response, String modelId,
            String mimeType) throws IOException {
        ModelSummary summary = modelSummaryEntityMgr.findValidByModelId(modelId);
        validateModelSummary(summary, modelId);
        String filePath = summary.getPivotArtifactPath();
        downloadFileByPath(request, response, mimeType, filePath);
    }

    @Override
    public void downloadTrainingSet(HttpServletRequest request, HttpServletResponse response, String modelId,
            String mimeType) throws IOException {
        ModelSummary summary = modelSummaryEntityMgr.findValidByModelId(modelId);
        validateModelSummary(summary, modelId);
        String trainingFilePath = summary.getModelSummaryConfiguration()
                .getString(ProvenancePropertyName.TrainingFilePath, "");
        downloadFileByPath(request, response, mimeType, trainingFilePath);
    }

    @Override
    public void downloadModelProfile(HttpServletRequest request, HttpServletResponse response, String modelId,
            String mimeType) throws IOException {
        ModelSummary summary = modelSummaryEntityMgr.findValidByModelId(modelId);
        validateModelSummary(summary, modelId);
        String modelProfilePath = String.format("%s%s/data/%s-Event-Metadata/%s", modelingServiceHdfsBaseDir,
                summary.getTenant().getId(), summary.getEventTableName(), MODEL_PROFILE_AVRO);
        downloadFileByPath(request, response, mimeType, modelProfilePath);
    }

    @Override
    public void downloadFileByApplicationId(HttpServletRequest request, HttpServletResponse response, String mimeType,
            String applicationId, String fileDisplayName) throws IOException {
        log.info(String.format("Download file with applicationId=%s", applicationId));
        SourceFile sourceFile = sourceFileEntityMgr.findByApplicationId(applicationId);
        validateSourceFile(sourceFile);
        downloadSourceFileCsv(request, response, mimeType, fileDisplayName, sourceFile);
    }

    @Override
    public void downloadFileByFileName(HttpServletRequest request, HttpServletResponse response, String mimeType,
            String fileName) throws IOException {
        log.info(String.format("Download file with fileName=%s", fileName));
        SourceFile sourceFile = sourceFileEntityMgr.findByName(fileName);
        validateSourceFile(sourceFile);
        downloadSourceFileCsv(request, response, mimeType, sourceFile.getDisplayName(), sourceFile);
    }

    private void validateSourceFile(SourceFile sourceFile) {
        if (sourceFile == null) {
            throw new NullPointerException("source file is null");
        }
    }

    @VisibleForTesting
    void downloadSourceFileCsv(HttpServletRequest request, HttpServletResponse response, String mimeType,
            String fileDisplayName, SourceFile sourceFile) throws IOException {
        boolean fileDownloaded = false;
        if (sourceFile != null) {
            String filePath = sourceFile.getPath();
            if (filePath != null && !filePath.isEmpty()) {
                CustomerSpaceHdfsFileDownloader downloader = getCustomerSpaceDownloader(mimeType, filePath,
                        fileDisplayName);
                downloader.downloadFile(request, response);
                fileDownloaded = true;
            }
        }

        if (!fileDownloaded) {
            throw new IOException(String.format("Error downloading source file with name: %s", fileDisplayName));
        }
    }

    @Override
    public void downloadFileByPath(HttpServletRequest request, HttpServletResponse response, String mimeType,
            String filePath) throws IOException {
        if (filePath != null && !filePath.isEmpty()) {
            CustomerSpaceHdfsFileDownloader downloader = getCustomerSpaceDownloader(mimeType, filePath, null);
            downloader.downloadFile(request, response);
        }
    }

    private CustomerSpaceHdfsFileDownloader getCustomerSpaceDownloader(String mimeType, String filePath,
            String fileName) {
        CustomerSpaceHdfsFileDownloader.FileDownloadBuilder builder = new CustomerSpaceHdfsFileDownloader.FileDownloadBuilder();
        builder.setMimeType(mimeType).setFilePath(filePath).setYarnConfiguration(yarnConfiguration)
                .setFileName(fileName);
        return new CustomerSpaceHdfsFileDownloader(builder);
    }

    @Override
    public String getFileContents(String modelId, String mimeType, String filter) throws Exception {
        HdfsFileHttpDownloader downloader = getDownloader(modelId, mimeType, filter);
        return downloader.getFileContents();
    }

    private HdfsFileHttpDownloader getDownloader(String modelId, String mimeType, String filter) {

        DownloadRequestBuilder requestBuilder = new DownloadRequestBuilder();
        requestBuilder.setMimeType(mimeType).setFilter(filter).setModelId(modelId)
                .setYarnConfiguration(yarnConfiguration);
        requestBuilder.setModelSummaryEntityMgr(modelSummaryEntityMgr)
                .setModelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir);
        return new HdfsFileHttpDownloader(requestBuilder);
    }

    private void validateModelSummary(ModelSummary summary, String modelId) {
        if (summary == null) {
            throw new NullPointerException(String.format("Modelsummary with id %s is null", modelId));
        }
    }

    @VisibleForTesting
    void setConfiguration(Configuration configuration) {
        this.yarnConfiguration = configuration;
    }

    @VisibleForTesting
    void setModelSummaryEntityMgr(ModelSummaryEntityMgr modelSummaryEntityMgr) {
        this.modelSummaryEntityMgr = modelSummaryEntityMgr;
    }

    @VisibleForTesting
    void setModelingServiceHdfsBaseDir(String modelingServiceHdfsBaseDir) {
        this.modelingServiceHdfsBaseDir = modelingServiceHdfsBaseDir;
    }
}
