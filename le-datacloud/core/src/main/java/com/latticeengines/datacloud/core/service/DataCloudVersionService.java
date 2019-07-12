package com.latticeengines.datacloud.core.service;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;

public interface DataCloudVersionService {
    List<DataCloudVersion> allVerions();

    DataCloudVersion latestApprovedForMajorVersion(String majorVersion);

    DataCloudVersion currentApprovedVersion();

    String nextMinorVersion(String version);

    List<String> priorVersions(String version, int num);

    void updateRefreshVersion();

    String currentDynamoVersion(String sourceName);
}
