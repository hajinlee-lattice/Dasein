package com.latticeengines.apps.cdl.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobDetail;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobType;

public interface CDLJobDetailDao extends BaseDao<CDLJobDetail> {

    List<CDLJobDetail> listAllRunningJobByJobType(CDLJobType cdlJobType);

    CDLJobDetail findLatestJobByJobType(CDLJobType cdlJobType);
}
