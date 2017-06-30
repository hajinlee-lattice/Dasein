package com.latticeengines.datacloud.yarn.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.swlib.exposed.service.SoftwareLibraryService;
import com.latticeengines.yarn.exposed.client.SingleContainerClientCustomization;

@Component("dataCoudClientCustomization")
public class DataCloudClientCustomization extends SingleContainerClientCustomization {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(DataCloudClientCustomization.class);

    @Autowired
    public DataCloudClientCustomization(Configuration yarnConfiguration, VersionManager versionManager,
            @Value("${dataplatform.hdfs.stack:}") String stackName, SoftwareLibraryService softwareLibraryService,
            @Value("${dataplatform.yarn.job.basedir}") String hdfsJobBaseDir,
            @Value("${hadoop.fs.web.defaultFS}") String webHdfs) {
        super(yarnConfiguration, versionManager, stackName, softwareLibraryService, hdfsJobBaseDir, webHdfs);
    }

    @Override
    public String getModuleName() {
        return "datacloud";
    }

}
