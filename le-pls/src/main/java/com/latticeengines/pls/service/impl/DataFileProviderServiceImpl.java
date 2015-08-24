package com.latticeengines.pls.service.impl;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

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

    @Override
    public void downloadFile(HttpServletRequest request, HttpServletResponse response, String modelId, String mimeType,
            String filter) throws IOException {

        HdfsFileHttpDownloader downloader = getDownloader(modelId, mimeType, filter);
        downloader.downloadFile(request, response);
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
