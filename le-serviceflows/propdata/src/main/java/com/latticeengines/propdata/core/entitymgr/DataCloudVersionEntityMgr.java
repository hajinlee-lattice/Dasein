package com.latticeengines.propdata.core.entitymgr;

import com.latticeengines.domain.exposed.propdata.manage.DataCloudVersion;

public interface DataCloudVersionEntityMgr {

    DataCloudVersion latestApprovedForMajorVersion(String majorVersion);

    DataCloudVersion findVersion(String version);

    DataCloudVersion createVersion(DataCloudVersion version);

    void deleteVersion(String version);
}
