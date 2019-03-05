package com.latticeengines.dataflowapi.yarn.client;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.swlib.exposed.service.SoftwareLibraryService;
import com.latticeengines.yarn.exposed.client.SingleContainerClientCustomization;

@Component("dataflowClientCustomization")
public class DataFlowClientCustomization extends SingleContainerClientCustomization {

    @Autowired
    public DataFlowClientCustomization(Configuration yarnConfiguration, //
            @Value("${dataplatform.hdfs.stack:}") String stackname,
            VersionManager versionManager, //
            SoftwareLibraryService softwareLibraryService, //
            @Value("${dataplatform.yarn.job.basedir}") String hdfsJobBaseDir, //
            @Value("${hadoop.fs.web.defaultFS}") String webHdfs) {
        super(yarnConfiguration, versionManager, stackname, softwareLibraryService, hdfsJobBaseDir, webHdfs);
    }

    @Override
    public String getModuleName() {
        return "dataflowapi";
    }

}
