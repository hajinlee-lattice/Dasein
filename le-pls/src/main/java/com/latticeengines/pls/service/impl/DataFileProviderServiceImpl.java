package com.latticeengines.pls.service.impl;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.pls.entitymanager.SourceFileEntityMgr;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.service.DataFileProviderService;
import com.latticeengines.pls.service.impl.HdfsFileHttpDownloader.DownloadRequestBuilder;

@Component("dataFileProviderService")
public class DataFileProviderServiceImpl implements DataFileProviderService {

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
    public void downloadPivotFile(HttpServletRequest request, HttpServletResponse response, String modelId, String mimeType) throws IOException {
        ModelSummary summary = modelSummaryEntityMgr.findValidByModelId(modelId);
        String filePath = summary.getPivotArtifactPath();
        downloadFileByPath(request, response, mimeType, filePath);
    }

    @Override
    public void downloadTrainingSet(HttpServletRequest request, HttpServletResponse response, String modelId, String
            mimeType) throws IOException {
        ModelSummary summary = modelSummaryEntityMgr.findValidByModelId(modelId);
        String trainingTableName = summary.getTrainingTableName();
        if (trainingTableName != null && !trainingTableName.isEmpty()) {
            SourceFile sourceFile = sourceFileEntityMgr.findByTableName(trainingTableName);
            if (sourceFile != null) {
                String filePath = sourceFile.getPath();
                downloadFileByPath(request, response, mimeType, filePath);
            }
        }
    }

    @Override
    public void downloadFileByPath(HttpServletRequest request, HttpServletResponse response, String mimeType,
            String filePath) throws IOException {
        if (filePath != null && !filePath.isEmpty()) {
            CustomerSpaceHdfsFileDownloader downloader = getDownloader(mimeType, filePath);
            downloader.downloadFile(request, response);
        }
    }


    private CustomerSpaceHdfsFileDownloader getDownloader(String mimeType, String filePath) {
        CustomerSpaceHdfsFileDownloader.FileDownloadBuilder builder = new CustomerSpaceHdfsFileDownloader.FileDownloadBuilder();
        builder.setMimeType(mimeType).setFilePath(filePath).setYarnConfiguration(yarnConfiguration);
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
        requestBuilder.setModelSummaryEntityMgr(modelSummaryEntityMgr).setModelingServiceHdfsBaseDir(
                modelingServiceHdfsBaseDir);
        return new HdfsFileHttpDownloader(requestBuilder);
    }

}
