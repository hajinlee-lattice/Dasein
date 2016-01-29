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
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.SourceFileEntityMgr;
import com.latticeengines.pls.service.FileUploadService;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.SecurityContextUtils;

@Component("fileUploadService")
public class FileUploadServiceImpl implements FileUploadService {

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private SourceFileEntityMgr sourceFileEntityMgr;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Override
    public SourceFile uploadFile(String outputFileName, InputStream inputStream) {
        try {
            Tenant tenant = SecurityContextUtils.getTenant();
            tenant = tenantEntityMgr.findByTenantId(tenant.getId());
            CustomerSpace space = CustomerSpace.parse(tenant.getId());
            String outputPath = PathBuilder.buildDataFilePath(CamilleEnvironment.getPodId(), space).toString();
            SourceFile file = new SourceFile();
            file.setTenant(tenant);
            file.setName(outputFileName);
            file.setPath(outputPath + "/" + outputFileName);
            HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, inputStream, outputPath + "/" + outputFileName);
            sourceFileEntityMgr.create(file);
            return file;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18053, e);
        }
    }

}
