package com.latticeengines.datacloud.yarn.client;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.swlib.exposed.service.SoftwareLibraryService;
import com.latticeengines.yarn.exposed.client.SingleContainerClientCustomization;

@Component("dataCoudClientCustomization")
public class DataCloudClientCustomization extends SingleContainerClientCustomization {

    @Inject
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
