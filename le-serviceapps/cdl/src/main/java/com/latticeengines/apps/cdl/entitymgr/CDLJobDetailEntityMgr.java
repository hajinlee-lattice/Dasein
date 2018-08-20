package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobDetail;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobType;

public interface CDLJobDetailEntityMgr extends BaseEntityMgrRepository<CDLJobDetail, Long> {

    List<CDLJobDetail> listAllRunningJobByJobType(CDLJobType cdlJobType);

    CDLJobDetail findLatestJobByJobType(CDLJobType cdlJobType);

    CDLJobDetail createJobDetail(CDLJobType cdlJobType,  Tenant tenant);

    void updateJobDetail(CDLJobDetail cdlJobDetail);

}
