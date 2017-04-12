package com.latticeengines.datacloud.core.entitymgr;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;


public interface DataCloudVersionEntityMgr {

    DataCloudVersion currentApprovedVersion();

    String currentApprovedVersionAsString();

    DataCloudVersion latestApprovedForMajorVersion(String majorVersion);

    DataCloudVersion findVersion(String version);

    DataCloudVersion createVersion(DataCloudVersion version);

    void deleteVersion(String version);

    List<DataCloudVersion> allVerions();

    List<String> allApprovedMajorVersions();
}
