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

    private HdfsFileHttpDownloader getDownloader(String modelId, String mimeType, String filter) {
        
        HdfsFileHttpDownloader downloader = new HdfsFileHttpDownloader(mimeType, filter);
        downloader.setModelId(modelId).setModelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir)
                .setModelSummaryEntityMgr(modelSummaryEntityMgr).setYarnConfiguration(yarnConfiguration);
        return downloader;
    }

}
