package com.latticeengines.propdata.core.service.impl;

import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.ZooDefs;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.propdata.core.service.ServiceFlowsZkConfigService;
import com.latticeengines.propdata.core.source.Source;

@Component("serviceFlowsZkConfigService")
public class ServiceFlowsZkConfigServiceImpl implements ServiceFlowsZkConfigService {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(ServiceFlowsZkConfigServiceImpl.class);

    private Camille camille;
    private String podId;
    private static final String PROPDATA_SERVICE = "PropData";
    private static final String SOURCES = "Sources";
    private static final String JOB_ENABLED = "JobEnabled";
    private static final String JOB_CRON = "JobCronExpression";

    @Autowired
    List<Source> sources;

    @Value("${propdata.source.db.json:source_dbs_dev.json}")
    private String sourceDbsJson;

    @Value("${propdata.target.db.json:target_dbs_dev.json}")
    private String targetDbsJson;

    @Value("${propdata.job.default.schedule:}")
    String defaultCron;

    @PostConstruct
    private void postConstruct() throws Exception {
        camille = CamilleEnvironment.getCamille();
        podId = CamilleEnvironment.getPodId();

        for (Source source : sources) {
            Path flagPath = jobFlagPath(source);
            if (!camille.exists(flagPath)) {
                camille.create(flagPath, ZooDefs.Ids.OPEN_ACL_UNSAFE);
            }

            Path cronPath = jobCronPath(source);
            if (StringUtils.isNotEmpty(source.getDefaultCronExpression())) {
                camille.upsert(cronPath, new Document(source.getDefaultCronExpression()), ZooDefs.Ids.OPEN_ACL_UNSAFE);
            }
        }
    }

    @Override
    public boolean refreshJobEnabled(Source source) {
        Path flagPath = jobFlagPath(source);
        try {
            if (camille.exists(flagPath)) {
                return Boolean.valueOf(camille.get(flagPath).getData());
            } else {
                camille.create(flagPath, ZooDefs.Ids.OPEN_ACL_UNSAFE);
                return false;
            }
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public String refreshCronSchedule(Source source) {
        Path cronPath = jobCronPath(source);
        try {
            if (camille.exists(cronPath)) {
                return String.valueOf(camille.get(cronPath).getData());
            } else if (StringUtils.isNotEmpty(source.getDefaultCronExpression())) {
                camille.create(cronPath, new Document(source.getDefaultCronExpression()), ZooDefs.Ids.OPEN_ACL_UNSAFE);
                return source.getDefaultCronExpression();
            } else {
                camille.create(cronPath, new Document(defaultCron), ZooDefs.Ids.OPEN_ACL_UNSAFE);
                return defaultCron;
            }
        } catch (Exception e) {
            return null;
        }
    }

    private Path jobFlagPath(Source source) {
        Path propDataPath = PathBuilder.buildServicePath(podId, PROPDATA_SERVICE);
        Path sourcePath = propDataPath.append(SOURCES).append(source.getSourceName());
        return sourcePath.append(JOB_ENABLED);
    }

    private Path jobCronPath(Source source) {
        Path propDataPath = PathBuilder.buildServicePath(podId, PROPDATA_SERVICE);
        Path sourcePath = propDataPath.append(SOURCES).append(source.getSourceName());
        return sourcePath.append(JOB_CRON);
    }

}
