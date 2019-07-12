package com.latticeengines.datacloud.core.service.impl;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.ACCOUNT_MASTER;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.ACCOUNT_MASTER_LOOKUP;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.DUNS_GUIDE_BOOK;

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


    @Override
    public List<DataCloudVersion> allVerions() {
        return versionEntityMgr.allVerions();
    }

    @Override
    public DataCloudVersion currentApprovedVersion() {
        return latestApprovedForMajorVersion(latestMajorVersion);
    }

    @Override
    public DataCloudVersion latestApprovedForMajorVersion(String version) {
        String majorVersion = DataCloudVersion.parseMajorVersion(version);
        return versionEntityMgr.latestApprovedForMajorVersion(majorVersion);
    }

    @Override
    public String nextMinorVersion(String version) {
        if (StringUtils.isBlank(version)) {
            return null;
        }
        String majorVersion = DataCloudVersion.parseMajorVersion(version);
        String minorVersion = DataCloudVersion.parseMinorVersion(version);
        return majorVersion + "." + (Integer.valueOf(minorVersion) + 1);
    }

    @Override
    public List<String> priorVersions(String version, int num) {
        List<String> list = new ArrayList<>();
        String majorVersion = DataCloudVersion.parseMajorVersion(version);
        int minorVersion = Integer.valueOf(DataCloudVersion.parseMinorVersion(version));
        for (int i = 0; i < num && i < minorVersion; i++) {
            list.add(majorVersion + "." + String.valueOf(minorVersion - i));
        }
        return list;
    }

    @Override
    public void updateRefreshVersion() {
        versionEntityMgr.updateRefreshVersion();
    }

    @Override
    public String currentDynamoVersion(String sourceName) {
        DataCloudVersion latestVersion = latestApprovedForMajorVersion(latestMajorVersion);
        switch (sourceName) {
        case ACCOUNT_MASTER:
            return constructDynamoVersion(latestVersion.getVersion(), latestVersion.getDynamoTableSignature());
        case ACCOUNT_MASTER_LOOKUP:
            return constructDynamoVersion(latestVersion.getVersion(), latestVersion.getDynamoTableSignatureLookup());
        case DUNS_GUIDE_BOOK:
            return constructDynamoVersion(latestVersion.getVersion(),
                    latestVersion.getDynamoTableSignatureDunsGuideBook());
        default:
            throw new UnsupportedOperationException(sourceName + " is not available in Dynamo");
        }
    }

    private String constructDynamoVersion(String datacloudVersion, String dynamoSignature) {
        if (StringUtils.isBlank(dynamoSignature)) {
            return datacloudVersion;
        } else {
            return datacloudVersion + "_" + dynamoSignature;
        }
    }
}
