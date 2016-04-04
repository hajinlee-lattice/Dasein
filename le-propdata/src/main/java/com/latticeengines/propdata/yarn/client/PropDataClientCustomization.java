package com.latticeengines.propdata.yarn.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.dataplatform.exposed.yarn.client.SingleContainerClientCustomization;

@Component("propDataClientCustomization")
public class PropDataClientCustomization extends SingleContainerClientCustomization {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(PropDataClientCustomization.class);

    @Autowired
    public PropDataClientCustomization(Configuration yarnConfiguration, VersionManager versionManager,
                                  @Value("${dataplatform.yarn.job.basedir}") String hdfsJobBaseDir,
                                  @Value("${dataplatform.fs.web.defaultFS}") String webHdfs) {
        super(yarnConfiguration, versionManager, hdfsJobBaseDir, webHdfs);
    }

    @Override
    public String getModuleName() {
        return "propdata";
    }

}
