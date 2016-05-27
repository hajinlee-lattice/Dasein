package com.latticeengines.scoring.yarn.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.dataplatform.exposed.yarn.client.SingleContainerClientCustomization;

@Component("scoringClientCustomization")
public class ScoringClientCustomization extends SingleContainerClientCustomization {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(ScoringClientCustomization.class);

    @Autowired
    public ScoringClientCustomization(Configuration yarnConfiguration, VersionManager versionManager,
            @Value("${dataplatform.hdfs.stack:}") String stackname,
            @Value("${dataplatform.yarn.job.basedir}") String hdfsJobBaseDir,
            @Value("${dataplatform.fs.web.defaultFS}") String webHdfs) {
        super(yarnConfiguration, versionManager, stackname, hdfsJobBaseDir, webHdfs);
        yarnConfiguration.setBoolean("mapreduce.job.user.classpath.first", true);
    }

    @Override
    public String getModuleName() {
        return "scoring";
    }

}