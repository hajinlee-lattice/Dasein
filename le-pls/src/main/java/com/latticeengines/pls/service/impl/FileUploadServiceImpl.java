package com.latticeengines.pls.service.impl;

import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.service.FileUploadService;
import com.latticeengines.security.exposed.util.SecurityContextUtils;

@Component("fileUploadService")
public class FileUploadServiceImpl implements FileUploadService {
    
    @Autowired
    private Configuration yarnConfiguration;

    @Override
    public void uploadFile(String outputFileName, InputStream inputStream) {
        try {
            Tenant tenant = SecurityContextUtils.getTenant();
            CustomerSpace space = CustomerSpace.parse(tenant.getId());
            String outputPath = PathBuilder.buildDataFilePath(CamilleEnvironment.getPodId(), space).toString();
            HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, inputStream, outputPath + "/" + outputFileName);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18053, e);
        }
    }

}
