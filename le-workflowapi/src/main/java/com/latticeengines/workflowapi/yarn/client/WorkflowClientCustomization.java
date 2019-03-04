package com.latticeengines.workflowapi.yarn.client;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.swlib.exposed.service.SoftwareLibraryService;
import com.latticeengines.yarn.exposed.client.SingleContainerClientCustomization;

@Component("workflowClientCustomization")
public class WorkflowClientCustomization extends SingleContainerClientCustomization {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(WorkflowClientCustomization.class);

    @Autowired
    public WorkflowClientCustomization(Configuration yarnConfiguration, VersionManager versionManager,
            @Value("${dataplatform.hdfs.stack:}") String stackname,
            SoftwareLibraryService softwareLibraryService,
            @Value("${dataplatform.yarn.job.basedir}") String hdfsJobBaseDir,
            @Value("${hadoop.fs.web.defaultFS}") String webHdfs) {
        super(yarnConfiguration, versionManager, stackname, softwareLibraryService, hdfsJobBaseDir, webHdfs);
    }

    @Override
    public String getModuleName() {
        return "workflowapi";
    }

}
