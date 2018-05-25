package com.latticeengines.datacloud.core.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.core.service.DataCloudVersionService;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;

@Component("dataCloudVersionService")
public class DataCloudVersionServiceImpl implements DataCloudVersionService {

    @Autowired
    private DataCloudVersionEntityMgr versionEntityMgr;

    @Value("${datacloud.match.latest.data.cloud.major.version}")
    private String latestMajorVersion;


    public List<DataCloudVersion> allVerions() {
        return versionEntityMgr.allVerions();
    }

    public DataCloudVersion currentApprovedVersion() {
        return latestApprovedForMajorVersion(latestMajorVersion);
    }

    public DataCloudVersion latestApprovedForMajorVersion(String version) {
        String majorVersion = DataCloudVersion.parseMajorVersion(version);
        return versionEntityMgr.latestApprovedForMajorVersion(majorVersion);
    }

    public String nextMinorVersion(String version) {
        if (StringUtils.isBlank(version)) {
            return null;
        }
        String majorVersion = DataCloudVersion.parseMajorVersion(version);
        String minorVersion = DataCloudVersion.parseMinorVersion(version);
        return majorVersion + "." + (Integer.valueOf(minorVersion) + 1);
    }

    public List<String> priorVersions(String version, int num) {
        List<String> list = new ArrayList<>();
        String majorVersion = DataCloudVersion.parseMajorVersion(version);
        int minorVersion = Integer.valueOf(DataCloudVersion.parseMinorVersion(version));
        for (int i = 0; i < num && i < minorVersion; i++) {
            list.add(majorVersion + "." + String.valueOf(minorVersion - i));
        }
        return list;
    }

    public void updateRefreshVersion() {
        versionEntityMgr.updateRefreshVersion();
    }
}
