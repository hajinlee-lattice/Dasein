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
        String majorVersion = parseMajorVersion(version);
        return versionEntityMgr.latestApprovedForMajorVersion(majorVersion);
    }

    public String nextMinorVersion(String version) {
        if (StringUtils.isBlank(version)) {
            return null;
        }
        String majorVersion = parseMajorVersion(version);
        String minorVersion = parseMinorVersion(version);
        return majorVersion + "." + (Integer.valueOf(minorVersion) + 1);
    }

    public List<String> priorVersions(String version, int num) {
        List<String> list = new ArrayList<>();
        String majorVersion = parseMajorVersion(version);
        int minorVersion = Integer.valueOf(parseMinorVersion(version));
        for (int i = 0; i < num && i < minorVersion; i++) {
            list.add(majorVersion + "." + String.valueOf(minorVersion - i));
        }
        return list;
    }

    private String parseMajorVersion(String version) {
        String[] tokens = version.split("\\.");
        if (tokens.length < 2) {
            throw new RuntimeException("Cannot parse a major version from " + version);
        }
        return tokens[0] + "." + tokens[1];
    }

    private String parseMinorVersion(String version) {
        String[] tokens = version.split("\\.");
        if (tokens.length < 3) {
            throw new RuntimeException("Cannot parse a minor version from " + version);
        }
        return tokens[2];
    }

    public void updateRefreshVersion() {
        versionEntityMgr.updateRefreshVersion();
    }
}
