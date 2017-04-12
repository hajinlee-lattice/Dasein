package com.latticeengines.datacloud.core.service.impl;

import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.core.service.DataCloudVersionService;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;

@Component("dataCloudVersionService")
public class DataCloudVersionServiceImpl implements DataCloudVersionService {

    private Log log = LogFactory.getLog(DataCloudVersionServiceImpl.class);

    @Autowired
    private DataCloudVersionEntityMgr versionEntityMgr;

    @Value("${datacloud.match.latest.data.cloud.major.version}")
    private String latestMajorVersion;
    
    @Value("${datacloud.match.approvedversion.refresh.minute:23}")
    private long refreshInterval;

    private ConcurrentMap<String, DataCloudVersion> latestApprovedMajorVersions = new ConcurrentHashMap<>();

    @Autowired
    @Qualifier("taskScheduler")
    private ThreadPoolTaskScheduler scheduler;

    @PostConstruct
    private void postConstruct() {
        loadCaches();
        scheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                loadCaches();
            }
        }, new Date(System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(refreshInterval)),
                TimeUnit.MINUTES.toMillis(refreshInterval));
    }

    public List<DataCloudVersion> allVerions() {
        return versionEntityMgr.allVerions();
    }

    public DataCloudVersion currentApprovedVersion() {
        return latestApprovedForMajorVersion(latestMajorVersion);
    }

    public DataCloudVersion latestApprovedForMajorVersion(String version) {
        String majorVersion = parseMajorVersion(version);
        if (latestApprovedMajorVersions.containsKey(majorVersion)) {
            return latestApprovedMajorVersions.get(majorVersion);
        } else {
            return versionEntityMgr.latestApprovedForMajorVersion(majorVersion);
        }
    }

    private void loadCaches() {
        ConcurrentMap<String, DataCloudVersion> latestApprovedMajorVersionsBak = latestApprovedMajorVersions;
        try {
            ConcurrentMap<String, DataCloudVersion> latestApprovedMajorVersionsNew = new ConcurrentHashMap<>();
            List<String> majorVersions = versionEntityMgr.allApprovedMajorVersions();
            for (String majorVersion : majorVersions) {
                latestApprovedMajorVersionsNew.put(majorVersion,
                        versionEntityMgr.latestApprovedForMajorVersion(majorVersion));
            }
            latestApprovedMajorVersions = latestApprovedMajorVersionsNew;
            log.info("Loaded latest approved version into cache");
        } catch (Exception ex) {
            log.error("Failed to refresh latest approved versions", ex);
            latestApprovedMajorVersions = latestApprovedMajorVersionsBak;
        }
    }

    private String parseMajorVersion(String version) {
        String[] tokens = version.split("\\.");
        if (tokens.length < 2) {
            throw new RuntimeException("Cannot parse a major version from " + version);
        }
        return tokens[0] + "." + tokens[1];
    }
}
