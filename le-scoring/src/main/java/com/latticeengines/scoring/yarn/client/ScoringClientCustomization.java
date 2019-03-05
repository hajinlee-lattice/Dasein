package com.latticeengines.scoring.yarn.client;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.yarn.exposed.client.SingleContainerClientCustomization;

@Component("scoringClientCustomization")
public class ScoringClientCustomization extends SingleContainerClientCustomization {

    @Autowired
    public ScoringClientCustomization(Configuration yarnConfiguration, VersionManager versionManager,
            @Value("${dataplatform.hdfs.stack:}") String stackname,
            @Value("${dataplatform.yarn.job.basedir}") String hdfsJobBaseDir,
            @Value("${hadoop.fs.web.defaultFS}") String webHdfs) {
        super(yarnConfiguration, versionManager, stackname, hdfsJobBaseDir, webHdfs);
        yarnConfiguration.setBoolean("mapreduce.job.user.classpath.first", true);
    }

    @Override
    public String getModuleName() {
        return "scoring";
    }

}
