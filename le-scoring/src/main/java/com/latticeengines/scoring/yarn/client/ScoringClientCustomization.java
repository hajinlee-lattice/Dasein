package com.latticeengines.scoring.yarn.client;

import java.util.Collection;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.yarn.fs.LocalResourcesFactoryBean;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.TransferEntry;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.dataplatform.exposed.yarn.client.SingleContainerClientCustomization;
import com.latticeengines.domain.exposed.scoring.RTSBulkScoringConfiguration;
import com.latticeengines.domain.exposed.scoring.RTSBulkScoringProperty;

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

//    @Override
//    public Collection<TransferEntry> getHdfsEntries(Properties containerProperties) {
//        Collection<LocalResourcesFactoryBean.TransferEntry> hdfsEntries = super.getHdfsEntries(containerProperties);
//        String rtsBulkScoringConfig = containerProperties.getProperty(RTSBulkScoringProperty.RTS_BULK_SCORING_CONFIG);
//        RTSBulkScoringConfiguration config = JsonUtils.deserialize(rtsBulkScoringConfig, RTSBulkScoringConfiguration.class);
//        String importErrorPath = config.getImportErrorPath();
//        if (StringUtils.isNotEmpty(importErrorPath)) {
//            hdfsEntries.add(new LocalResourcesFactoryBean.TransferEntry(LocalResourceType.FILE, //
//                    LocalResourceVisibility.PUBLIC, //
//                    importErrorPath, //
//                    false));
//        }
//        return hdfsEntries;
//    }

}