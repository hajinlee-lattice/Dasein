package com.latticeengines.propdata.core.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;

public interface DataCloudVersionDao extends BaseDao<DataCloudVersion>  {

    DataCloudVersion latestApprovedForMajorVersion(String majorVersion);

}
