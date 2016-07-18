package com.latticeengines.modelquality.service.impl;

import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.modelquality.service.PipelineService;

@Component("pipelineService")
public class PipelineServiceImpl implements PipelineService {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(PipelineServiceImpl.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Value("${modelquality.file.upload.hdfs.dir}")
    private String hdfsDir;

    @Override
    public String uploadFile(String fileName, MultipartFile file) {
        try {
            InputStream inputStream = file.getInputStream();
            String hdfsPath = hdfsDir + "/" + fileName;
            HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, inputStream, hdfsPath);
            return hdfsPath;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
